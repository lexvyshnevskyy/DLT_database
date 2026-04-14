from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Callable, Dict

import rclpy
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
        self.declare_parameter('auto_init_schema', True)

        connector = DbConnector(
            host=self.get_parameter('db.host').value,
            port=int(self.get_parameter('db.port').value),
            user=self.get_parameter('db.user').value,
            password=self.get_parameter('db.password').value,
            database=self.get_parameter('db.name').value,
            logger=self.get_logger(),
        )
        DbControl.__init__(self, connector)

        if bool(self.get_parameter('auto_init_schema').value):
            schema_path = self._find_schema_path()
            if schema_path is not None:
                self.db.initialize_schema(str(schema_path))
            else:
                self.get_logger().warning('Schema file not found in source or installed locations.')

        self.command_dispatch: Dict[str, Callable[[Dict[str, Any]], Dict[str, Any]]] = {
            'new_program': self.handler_add_program,
            'get_program_by_id': self.handler_get_program_by_id,
            'program_all_list': self.handler_get_program_all,
            'program_delete_by_id': self.handle_program_delete_by_id,
            'program_update_status': self.handle_program_update_status,
            'program_step_list': self.handle_program_step_list,
            'program_step_insert': self.handle_program_step_insert,
            'program_step_update': self.handle_program_step_update,
            'program_delete_temp': self.handle_program_delete_temp,
            'set_e720': self.handle_set_e720,
            'get_e720': self.handle_get_e720,
            'measurement_insert': self.handle_measurement_insert,
            'measurement_bulk_insert': self.handle_measurement_bulk_insert,
            'measurement_list': self.handle_measurement_list,
            'measurement_delete_by_program_id': self.handle_measurement_delete_by_program_id,
            'measurement_stats': self.handle_measurement_stats,
        }

        self.service = self.create_service(Query, 'query', self.handle_query)
        self.get_logger().info('Service [/database/query] is ready.')

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
        self.get_logger().info(f'Received query: {request.query}')
        try:
            query_dict = json.loads(request.query)
            result = self.process_query(query_dict)
        except Exception as exc:
            result = {'result': 'False', 'error': str(exc)}
        response.response = json.dumps(result)
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
            return {'result': 'False', 'ID': '0'}
        except Exception as exc:
            return {'result': 'False', 'ID': '0', 'error': str(exc)}

    def handle_measurement_insert(self, val: Dict[str, Any]) -> Dict[str, Any]:
        try:
            response = self.add_measurement(val)
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
            limit = int(val.get('limit', 1000))
            response = self.get_measurements(program_id, limit)
            rows = [
                {
                    'id': item_id,
                    'program_id': row_program_id,
                    'elapsed_s': elapsed_s,
                    'freq': freq,
                    'measure_ch1': measure_ch1,
                    'measure_ch2': measure_ch2,
                    't_ch1': t_ch1,
                    't_ch2': t_ch2,
                    't_exp': t_exp,
                    'created_at': created_at.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3],
                }
                for item_id, row_program_id, elapsed_s, freq, measure_ch1, measure_ch2, t_ch1, t_ch2, t_exp, created_at in response
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
            response = self.get_measurement_stats(program_id)
            return {'result': 'Ok', 'row': response}
        except Exception as exc:
            return {'result': 'False', 'row': {}, 'error': str(exc)}


def main(args=None) -> None:
    rclpy.init(args=args)
    node = DbService()
    try:
        rclpy.spin(node)
    finally:
        node.db.close()
        node.destroy_node()
        rclpy.shutdown()


if __name__ == '__main__':
    main()
