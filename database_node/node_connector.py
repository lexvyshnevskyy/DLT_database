from __future__ import annotations

import json
import threading
import traceback
from pathlib import Path
from typing import Any, Callable, Dict, List

import rclpy
from rclpy.callback_groups import ReentrantCallbackGroup
from rclpy.executors import MultiThreadedExecutor
from rclpy.node import Node

from database.srv import Query

from .connector import DbConnector
from .db_control import DbControl


class DbService(Node, DbControl):
    def __init__(self) -> None:
        Node.__init__(self, 'database')
        self.declare_parameter('publish_rate', 100.0)
        self.declare_parameter('response_endpoint', 'response')
        self.declare_parameter('query_endpoint', 'query')
        self.declare_parameter('db.host', '127.0.0.1')
        self.declare_parameter('db.port', 3306)
        self.declare_parameter('db.user', 'ubuntu')
        self.declare_parameter('db.password', 'raspberry')
        self.declare_parameter('db.name', 'exp')
        self.declare_parameter('db.pool_size', 10)
        self.declare_parameter('auto_init_schema', True)

        connector = DbConnector(
            host=self.get_parameter('db.host').value,
            port=int(self.get_parameter('db.port').value),
            user=self.get_parameter('db.user').value,
            password=self.get_parameter('db.password').value,
            database=self.get_parameter('db.name').value,
            pool_size=int(self.get_parameter('db.pool_size').value),
            logger=self.get_logger(),
        )
        DbControl.__init__(self, connector)

        if bool(self.get_parameter('auto_init_schema').value):
            schema_path = self._find_schema_path()
            if schema_path is not None:
                self.db.initialize_schema(str(schema_path))
            else:
                self._safe_log_warning('Schema file not found in source or installed locations.')
        try:
            self.ensure_program_run_schema()
        except Exception as exc:
            self._safe_log_warning(f'program_runs schema migration: {exc}')

        self.command_dispatch: Dict[str, Callable[[Dict[str, Any]], Dict[str, Any]]] = {
            'new_program': self.handler_add_program,
            'get_program_by_id': self.handler_get_program_by_id,
            'program_all_list': self.handler_get_program_all,
            'program_all_list_with_counts': self.handler_get_program_all_with_counts,
            'program_delete_by_id': self.handle_program_delete_by_id,
            'program_update_status': self.handle_program_update_status,
            'program_step_list': self.handle_program_step_list,
            'program_step_insert': self.handle_program_step_insert,
            'program_step_update': self.handle_program_step_update,
            'program_delete_temp': self.handle_program_delete_temp,
            'set_e720': self.handle_set_e720,
            'get_e720': self.handle_get_e720,
            'set_program_meta': self.handle_set_program_meta,
            'get_program_detail': self.handle_get_program_detail,
            'measurement_insert': self.handle_measurement_insert,
            'measurement_bulk_insert': self.handle_measurement_bulk_insert,
            'measurement_list': self.handle_measurement_list,
            'measurement_delete_by_program_id': self.handle_measurement_delete_by_program_id,
            'measurement_stats': self.handle_measurement_stats,
            'program_run_start': self.handle_program_run_start,
            'program_run_finish': self.handle_program_run_finish,
            'program_run_list': self.handle_program_run_list,
            'program_run_counts': self.handle_program_run_counts,
            'program_run_finish_active': self.handle_program_run_finish_active,
            'program_run_delete': self.handle_program_run_delete,
            'program_run_get': self.handle_program_run_get,
            'measurement_list_page': self.handle_measurement_list_page,
            'measurement_run_frequencies': self.handle_measurement_run_frequencies,
        }

        # Protect only small in-memory state (elapsed-time anchors).
        # DB connections/cursors are now thread-local in DbConnector, so reads
        # and writes can run from multiple ROS service worker threads safely.
        self._state_lock = threading.RLock()
        self._service_cb_group = ReentrantCallbackGroup()
        self.service = self.create_service(
            Query,
            'query',
            self.handle_query,
            callback_group=self._service_cb_group,
        )
        self._safe_log_info('Service [/database/query] is ready.')

    def _safe_log_info(self, message: str) -> None:
        try:
            self.get_logger().info(message)
        except Exception:
            print(message, flush=True)

    def _safe_log_warning(self, message: str) -> None:
        # Use info on purpose: see DbConnector._log() note about rcutils severity
        # caching.  The "[WARNING]" prefix keeps the visible severity meaning.
        try:
            self.get_logger().info(f'[WARNING] {message}')
        except Exception:
            print(f'[WARNING] {message}', flush=True)

    def _safe_log_error(self, message: str) -> None:
        # Use info on purpose: see DbConnector._log() note about rcutils severity
        # caching.  The "[ERROR]" prefix keeps the visible severity meaning.
        try:
            self.get_logger().info(f'[ERROR] {message}')
        except Exception:
            print(f'[ERROR] {message}', flush=True)

    @staticmethod
    def _find_schema_path() -> Path | None:
        current = Path(__file__).resolve()
        candidates = [
            current.parent.parent / 'sql' / 'schema.sql',
            current.parents[2] / 'share' / 'database' / 'sql' / 'schema.sql',
        ]
        for candidate in candidates:
            if candidate.exists():
                return candidate
        return None

    def handle_query(self, request: Query.Request, response: Query.Response) -> Query.Response:
        try:
            query_dict = json.loads(request.query)
        except json.JSONDecodeError as exc:
            query_dict = {}
            self._safe_log_error(f'Invalid database query JSON: {exc}; raw={request.query!r}')

        cmd = str(query_dict.get('cmd', ''))
        if cmd not in (
            'measurement_insert',
            'measurement_bulk_insert',
            'measurement_list',
            'measurement_list_page',
        ):
            self._safe_log_info(f'Received query: {request.query}')

        try:
            result = self.process_query(query_dict)
        except Exception as exc:
            trace = traceback.format_exc(limit=6)
            self._safe_log_error(f'Database query failed: cmd={cmd!r}; error={exc}; trace={trace}')
            result = {
                'result': 'False',
                'error': str(exc),
                'error_type': type(exc).__name__,
            }
        response.response = json.dumps(result, default=str)
        return response

    def process_query(self, query: Dict[str, Any]) -> Dict[str, Any]:
        callback = self.command_dispatch.get(query.get('cmd'))
        sliced = {k: v for k, v in query.items() if k != 'cmd'}
        if callback:
            return callback(sliced)
        return {'result': 'False', 'error': f"No handler for command: {query.get('cmd')}"}

    def handler_add_program(self, _val=None) -> Dict[str, Any]:
        response = self.add_program()
        if response > 0:
            return {'result': 'Ok', 'ID': response}
        return {'result': 'False', 'ID': '0'}

    def handler_get_program_by_id(self, val: Dict[str, Any]) -> Dict[str, Any]:
        try:
            response = self.get_program_by_id(int(val.get('id', 0)))
            item_id, dt, status = response
            result = f'{item_id}^{dt:%Y-%m-%d %H:%M:%S}^{status}'
            return {'result': 'Ok', 'row': result}
        except Exception as exc:
            return {'result': 'False', 'ID': '0', 'error': str(exc)}

    def handler_get_program_all(self, _val=None) -> Dict[str, Any]:
        try:
            response = self.get_all_programs()
            rows = [f'{value}^{dt:%Y-%m-%d %H:%M:%S}^{status}' for value, dt, status in response]
            return {'result': 'Ok', 'row': rows}
        except Exception as exc:
            return {'result': 'False', 'ID': '0', 'error': str(exc)}

    def handler_get_program_all_with_counts(self, _val=None) -> Dict[str, Any]:
        try:
            programs = self.get_all_programs()
            counts = self.program_run_counts_all()
            rows = [
                f'{value}^{dt:%Y-%m-%d %H:%M:%S}^{status}^{counts.get(int(value), 0)}'
                for value, dt, status in programs
            ]
            return {'result': 'Ok', 'row': rows}
        except Exception as exc:
            return {'result': 'False', 'ID': '0', 'error': str(exc)}

    def handle_program_delete_by_id(self, val: Dict[str, Any]) -> Dict[str, Any]:
        try:
            response = self.delete_program(int(val.get('id', 0)))
            if response > 0:
                return {'result': 'Ok', 'ID': response}
            return {'result': 'False', 'ID': '0'}
        except Exception as exc:
            return {'result': 'False', 'ID': '0', 'error': str(exc)}

    def handle_program_update_status(self, val: Dict[str, Any]) -> Dict[str, Any]:
        try:
            response = self.update_program_status(int(val.get('id', 0)), str(val.get('status', 'New')))
            if response > 0:
                return {'result': 'Ok', 'ID': response}
            return {'result': 'False', 'ID': '0'}
        except Exception as exc:
            return {'result': 'False', 'ID': '0', 'error': str(exc)}

    def handle_program_step_list(self, val: Dict[str, Any]) -> Dict[str, Any]:
        try:
            response = self.get_program_params_temp(int(val.get('id', 0)))
            rows = [f'{a}^{b}^{c}^{d}' for a, _, b, c, d in response]
            return {'result': 'Ok', 'row': rows}
        except Exception as exc:
            return {'result': 'False', 'ID': '0', 'error': str(exc)}

    def handle_set_e720(self, val: Dict[str, Any]) -> Dict[str, Any]:
        try:
            if self.add_e720(val):
                return {'result': 'Ok'}
            return {'result': 'False', 'ID': '0'}
        except Exception as exc:
            return {'result': 'False', 'ID': '0', 'error': str(exc)}

    def handle_get_e720(self, val: Dict[str, Any]) -> Dict[str, Any]:
        try:
            response = self.get_e720(int(val.get('id', 0)))
            if response:
                return {'result': 'Ok', 'row': response}
            return {'result': 'False', 'ID': '0'}
        except Exception as exc:
            return {'result': 'False', 'ID': '0', 'error': str(exc)}

    def handle_set_program_meta(self, val: Dict[str, Any]) -> Dict[str, Any]:
        try:
            program_id = int(val.get('program_id', val.get('id', 0)))
            key = str(val.get('key', '')).strip()
            value = str(val.get('value', ''))
            if not key:
                return {'result': 'False', 'error': 'meta key is required'}
            if self.set_program_meta(program_id, key, value):
                return {'result': 'Ok'}
            return {'result': 'False', 'error': 'failed to save meta'}
        except Exception as exc:
            return {'result': 'False', 'error': str(exc)}

    def handle_get_program_detail(self, val: Dict[str, Any]) -> Dict[str, Any]:
        try:
            program_id = int(val.get('id', val.get('program_id', 0)))
            detail = self.get_program_detail(program_id)
            if not detail:
                return {'result': 'False', 'error': f'program {program_id} not found'}
            return {'result': 'Ok', 'row': detail}
        except Exception as exc:
            return {'result': 'False', 'error': str(exc)}

    def handle_program_step_insert(self, val: Dict[str, Any]) -> Dict[str, Any]:
        try:
            response = self.set_program_temp(val)
            if response:
                return {'result': 'Ok', 'Id': response}
            return {'result': 'False', 'ID': '0'}
        except Exception as exc:
            return {'result': 'False', 'ID': '0', 'error': str(exc)}

    def handle_program_delete_temp(self, val: Dict[str, Any]) -> Dict[str, Any]:
        try:
            response = self.delete_program_temp(int(val.get('id', 0)))
            if response > 0:
                return {'result': 'Ok', 'ID': response}
            return {'result': 'False', 'ID': '0'}
        except Exception as exc:
            return {'result': 'False', 'ID': '0', 'error': str(exc)}

    def handle_program_step_update(self, val: Dict[str, Any]) -> Dict[str, Any]:
        try:
            response = self.update_program_temp(val)
            if response:
                return {'result': 'Ok', 'Id': response}
            sid = val.get('id', '?')
            return {'result': 'False', 'ID': '0', 'error': f'step {sid} not found'}
        except Exception as exc:
            return {'result': 'False', 'ID': '0', 'error': str(exc)}

    def handle_measurement_insert(self, val: Dict[str, Any]) -> Dict[str, Any]:
        try:
            response = self.add_measurement_pooled(val)
            if response > 0:
                return {'result': 'Ok', 'ID': response}
            return {'result': 'False', 'ID': '0'}
        except Exception as exc:
            return {'result': 'False', 'ID': '0', 'error': str(exc)}

    def handle_measurement_bulk_insert(self, val: Dict[str, Any]) -> Dict[str, Any]:
        try:
            rows = val.get('rows', [])
            response = self.add_measurements_bulk(rows)
            return {'result': 'Ok', 'count': response}
        except Exception as exc:
            return {'result': 'False', 'count': 0, 'error': str(exc)}

    def handle_measurement_list(self, val: Dict[str, Any]) -> Dict[str, Any]:
        try:
            program_id = int(val.get('program_id', val.get('exp_id', 0)))
            run_id = int(val.get('run_id', 0) or 0)
            limit = int(val.get('limit', 1000))
            offset = int(val.get('offset', 0) or 0)
            response = self.get_measurements(program_id, limit, run_id=run_id, offset=offset)
            rows = [
                {
                    'id': item_id,
                    'program_id': row_program_id,
                    'run_id': row_run_id,
                    'elapsed_s': elapsed_s,
                    'freq': freq,
                    'measure_ch1': measure_ch1,
                    'measure_ch2': measure_ch2,
                    't_ch1': t_ch1,
                    't_ch2': t_ch2,
                    't_exp': t_exp,
                    'created_at': created_at.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3],
                }
                for (
                    item_id,
                    row_program_id,
                    row_run_id,
                    elapsed_s,
                    freq,
                    measure_ch1,
                    measure_ch2,
                    t_ch1,
                    t_ch2,
                    t_exp,
                    created_at,
                ) in response
            ]
            return {'result': 'Ok', 'row': rows}
        except Exception as exc:
            return {'result': 'False', 'row': [], 'error': str(exc)}

    def handle_measurement_delete_by_program_id(self, val: Dict[str, Any]) -> Dict[str, Any]:
        try:
            program_id = int(val.get('program_id', val.get('exp_id', 0)))
            response = self.delete_measurements(program_id)
            return {'result': 'Ok', 'count': response}
        except Exception as exc:
            return {'result': 'False', 'count': 0, 'error': str(exc)}

    def handle_measurement_stats(self, val: Dict[str, Any]) -> Dict[str, Any]:
        try:
            program_id = int(val.get('program_id', val.get('exp_id', 0)))
            run_id = int(val.get('run_id', 0) or 0)
            if run_id > 0:
                response = self.get_measurement_stats(run_id=run_id)
            else:
                response = self.get_measurement_stats(program_id)
            return {'result': 'Ok', 'row': response}
        except Exception as exc:
            return {'result': 'False', 'row': {}, 'error': str(exc)}

    def handle_program_run_start(self, val: Dict[str, Any]) -> Dict[str, Any]:
        try:
            program_id = int(val.get('program_id', val.get('id', 0)))
            row = self.start_program_run(program_id)
            if not row:
                return {'result': 'False', 'error': 'failed to start program run'}
            return {'result': 'Ok', 'row': row}
        except Exception as exc:
            return {'result': 'False', 'error': str(exc)}

    def handle_program_run_finish(self, val: Dict[str, Any]) -> Dict[str, Any]:
        try:
            run_id = int(val.get('run_id', 0))
            status = str(val.get('status', 'Stopped'))
            affected = self.finish_program_run(run_id, status)
            if affected <= 0:
                return {'result': 'False', 'error': 'run not found'}
            return {'result': 'Ok', 'ID': run_id}
        except Exception as exc:
            return {'result': 'False', 'error': str(exc)}

    def handle_program_run_list(self, val: Dict[str, Any]) -> Dict[str, Any]:
        try:
            program_id = int(val.get('program_id', val.get('id', 0)))
            rows = self.list_program_runs(program_id)
            return {'result': 'Ok', 'row': rows}
        except Exception as exc:
            return {'result': 'False', 'row': [], 'error': str(exc)}

    def handle_program_run_counts(self, val: Dict[str, Any]) -> Dict[str, Any]:
        try:
            counts = self.program_run_counts_all()
            rows = [f'{program_id}^{count}' for program_id, count in sorted(counts.items())]
            return {'result': 'Ok', 'row': rows}
        except Exception as exc:
            return {'result': 'False', 'row': [], 'error': str(exc)}

    def handle_program_run_finish_active(self, val: Dict[str, Any]) -> Dict[str, Any]:
        try:
            program_id = int(val.get('program_id', val.get('id', 0)))
            count = self.finish_active_program_runs(program_id, str(val.get('status', 'Stopped')))
            return {'result': 'Ok', 'count': count}
        except Exception as exc:
            return {'result': 'False', 'count': 0, 'error': str(exc)}

    def handle_program_run_delete(self, val: Dict[str, Any]) -> Dict[str, Any]:
        try:
            run_id = int(val.get('run_id', 0))
            affected = self.delete_program_run(run_id)
            if affected <= 0:
                return {'result': 'False', 'error': 'run not found'}
            return {'result': 'Ok', 'ID': run_id}
        except Exception as exc:
            return {'result': 'False', 'error': str(exc)}

    def handle_program_run_get(self, val: Dict[str, Any]) -> Dict[str, Any]:
        try:
            run_id = int(val.get('run_id', 0))
            row = self.get_program_run_by_id(run_id)
            if not row:
                return {'result': 'False', 'error': 'run not found'}
            return {'result': 'Ok', 'row': row}
        except Exception as exc:
            return {'result': 'False', 'error': str(exc)}

    def handle_measurement_list_page(self, val: Dict[str, Any]) -> Dict[str, Any]:
        try:
            run_id = int(val.get('run_id', 0))
            offset = int(val.get('offset', 0) or 0)
            limit = int(val.get('limit', 100) or 100)
            program_id = int(val.get('program_id', 0) or 0)
            rows_raw = self.get_measurements(program_id, limit, run_id=run_id, offset=offset)
            total = self.count_measurements_for_run(run_id) if run_id > 0 else 0
            rows = self._measurement_rows_to_dicts(rows_raw)
            return {'result': 'Ok', 'row': rows, 'total': total, 'offset': offset, 'limit': limit}
        except Exception as exc:
            return {'result': 'False', 'row': [], 'total': 0, 'error': str(exc)}

    def handle_measurement_run_frequencies(self, val: Dict[str, Any]) -> Dict[str, Any]:
        try:
            run_id = int(val.get('run_id', 0))
            freqs = self.get_run_distinct_frequencies(run_id)
            return {'result': 'Ok', 'row': freqs}
        except Exception as exc:
            return {'result': 'False', 'row': [], 'error': str(exc)}

    @staticmethod
    def _measurement_rows_to_dicts(rows: List[Any]) -> List[Dict[str, Any]]:
        result = []
        for (
            item_id,
            row_program_id,
            row_run_id,
            elapsed_s,
            freq,
            measure_ch1,
            measure_ch2,
            t_ch1,
            t_ch2,
            t_exp,
            created_at,
        ) in rows:
            result.append({
                'id': item_id,
                'program_id': row_program_id,
                'run_id': row_run_id,
                'elapsed_s': elapsed_s,
                'freq': freq,
                'measure_ch1': measure_ch1,
                'measure_ch2': measure_ch2,
                't_ch1': t_ch1,
                't_ch2': t_ch2,
                't_exp': t_exp,
                'created_at': created_at.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
                if hasattr(created_at, 'strftime')
                else str(created_at),
            })
        return result


def main(args=None) -> None:
    rclpy.init(args=args)
    node = DbService()
    executor = MultiThreadedExecutor(num_threads=4)
    executor.add_node(node)
    try:
        executor.spin()
    finally:
        node.db.close()
        executor.shutdown()
        node.destroy_node()
        rclpy.shutdown()


if __name__ == '__main__':
    main()
