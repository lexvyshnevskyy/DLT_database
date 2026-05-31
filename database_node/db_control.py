from __future__ import annotations

import json
import threading
import time
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Tuple

from .connector import DbConnector


class DbControl:
    def __init__(self, connector: DbConnector):
        self.db = connector
        self._program_start_monotonic_ns: Dict[int, int] = {}
        self._run_start_monotonic_ns: Dict[int, int] = {}
        self._elapsed_lock = threading.RLock()

    def _scalar(self, query: str, params: Iterable[Any]) -> Optional[Any]:
        self.db.cur.execute(query, tuple(params))
        row = self.db.cur.fetchone()
        if row is None:
            return None
        return row[0]

    def _resolve_program_id(self, data: Dict[str, Any]) -> int:
        raw_value = data.get('program_id', data.get('exp_id', 0))
        return int(raw_value)

    def _resolve_run_id(self, data: Dict[str, Any]) -> int:
        return int(data.get('run_id', 0) or 0)

    def _table_exists(self, table_name: str) -> bool:
        value = self._scalar(
            """
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_schema = DATABASE() AND table_name = %s;
            """,
            (table_name,),
        )
        return bool(value)

    def _column_exists(self, table_name: str, column_name: str) -> bool:
        value = self._scalar(
            """
            SELECT COUNT(*)
            FROM information_schema.columns
            WHERE table_schema = DATABASE()
              AND table_name = %s
              AND column_name = %s;
            """,
            (table_name, column_name),
        )
        return bool(value)

    def ensure_program_run_schema(self) -> bool:
        """Apply program_runs table and measurements.run_id for existing databases."""
        if not self._table_exists('program_runs'):
            self.db.cur.execute(
                """
                CREATE TABLE program_runs (
                    id INT NOT NULL AUTO_INCREMENT,
                    program_id INT NOT NULL,
                    run_index INT NOT NULL,
                    started_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
                    stopped_at DATETIME(3) NULL,
                    status VARCHAR(32) NOT NULL DEFAULT 'Running',
                    PRIMARY KEY (id),
                    UNIQUE KEY uk_program_run_index (program_id, run_index),
                    KEY idx_program_runs_program_id (program_id),
                    KEY idx_program_runs_status (status),
                    CONSTRAINT fk_program_runs_program
                      FOREIGN KEY (program_id) REFERENCES programs(ID)
                      ON DELETE CASCADE
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
                """
            )
            self.db.conn.commit()

        if not self._column_exists('measurements', 'run_id'):
            self.db.cur.execute(
                'ALTER TABLE measurements ADD COLUMN run_id INT NULL AFTER program_id;'
            )
            self.db.conn.commit()
            self._backfill_measurement_run_ids()
            try:
                self.db.cur.execute(
                    """
                    ALTER TABLE measurements
                      ADD KEY idx_measurements_run_id (run_id),
                      ADD CONSTRAINT fk_measurements_run
                        FOREIGN KEY (run_id) REFERENCES program_runs(id)
                        ON DELETE CASCADE;
                    """
                )
                self.db.conn.commit()
            except Exception:
                self.db.conn.rollback()
        return True

    def _backfill_measurement_run_ids(self) -> None:
        self.db.cur.execute(
            'SELECT DISTINCT program_id FROM measurements WHERE run_id IS NULL ORDER BY program_id;'
        )
        program_ids = [int(row[0]) for row in self.db.cur.fetchall()]
        for program_id in program_ids:
            self.db.cur.execute(
                'SELECT MIN(created_at), MAX(created_at), COUNT(*) FROM measurements WHERE program_id = %s;',
                (program_id,),
            )
            started_at, stopped_at, count = self.db.cur.fetchone()
            run_id = self._create_program_run_row(
                program_id,
                run_index=1,
                started_at=started_at,
                stopped_at=stopped_at,
                status='Stopped',
            )
            if run_id <= 0:
                continue
            self.db.cur.execute(
                'UPDATE measurements SET run_id = %s WHERE program_id = %s AND run_id IS NULL;',
                (run_id, program_id),
            )
            self.db.conn.commit()
            _ = count

    def _create_program_run_row(
        self,
        program_id: int,
        *,
        run_index: int,
        started_at: Optional[datetime] = None,
        stopped_at: Optional[datetime] = None,
        status: str = 'Running',
    ) -> int:
        if started_at is None:
            started_at = datetime.now()
        sql = """
        INSERT INTO program_runs (program_id, run_index, started_at, stopped_at, status)
        VALUES (%s, %s, %s, %s, %s);
        """
        try:
            self.db.cur.execute(sql, (program_id, run_index, started_at, stopped_at, status))
            self.db.conn.commit()
            return int(self.db.cur.lastrowid)
        except Exception:
            self.db.conn.rollback()
            return 0

    def start_program_run(self, program_id: int) -> Dict[str, Any]:
        if not self._program_exists(program_id):
            return {}
        self.finish_active_program_runs(program_id, 'Stopped')
        next_index = int(self._scalar(
            'SELECT COALESCE(MAX(run_index), 0) + 1 FROM program_runs WHERE program_id = %s;',
            (program_id,),
        ) or 1)
        run_id = self._create_program_run_row(program_id, run_index=next_index, status='Running')
        if run_id <= 0:
            return {}
        self._run_start_monotonic_ns[run_id] = time.monotonic_ns()
        row = self.get_program_run_by_id(run_id)
        return row or {}

    def delete_program_run(self, run_id: int) -> int:
        query = 'DELETE FROM program_runs WHERE id = %s;'
        try:
            self.db.cur.execute(query, (run_id,))
            affected = int(self.db.cur.rowcount)
            self.db.conn.commit()
            self._run_start_monotonic_ns.pop(run_id, None)
            return affected
        except Exception:
            self.db.conn.rollback()
            return 0

    def finish_program_run(self, run_id: int, final_status: str = 'Stopped') -> int:
        if run_id <= 0:
            return 0
        sql = """
        UPDATE program_runs
        SET status = %s, stopped_at = COALESCE(stopped_at, CURRENT_TIMESTAMP(3))
        WHERE id = %s;
        """
        try:
            self.db.cur.execute(sql, (final_status, run_id))
            affected = int(self.db.cur.rowcount)
            self.db.conn.commit()
            self._run_start_monotonic_ns.pop(run_id, None)
            return affected
        except Exception:
            self.db.conn.rollback()
            return 0

    def finish_active_program_runs(self, program_id: int, final_status: str = 'Stopped') -> int:
        self.db.cur.execute(
            """
            SELECT id FROM program_runs
            WHERE program_id = %s AND status = 'Running'
            ORDER BY id;
            """,
            (program_id,),
        )
        run_ids = [int(row[0]) for row in self.db.cur.fetchall()]
        total = 0
        for run_id in run_ids:
            total += self.finish_program_run(run_id, final_status)
        return total

    def get_program_run_by_id(self, run_id: int) -> Dict[str, Any]:
        query = """
        SELECT id, program_id, run_index, started_at, stopped_at, status
        FROM program_runs
        WHERE id = %s;
        """
        self.db.cur.execute(query, (run_id,))
        row = self.db.cur.fetchone()
        if row is None:
            return {}
        return self._program_run_dict(row)

    def _program_run_dict(self, row: Tuple[Any, ...]) -> Dict[str, Any]:
        run_id, program_id, run_index, started_at, stopped_at, status = row
        started_text = started_at.isoformat(sep=' ', timespec='seconds') if hasattr(started_at, 'isoformat') else str(started_at)
        stopped_text = None
        if stopped_at is not None:
            stopped_text = (
                stopped_at.isoformat(sep=' ', timespec='seconds')
                if hasattr(stopped_at, 'isoformat')
                else str(stopped_at)
            )
        stats = self.get_measurement_stats(run_id=int(run_id))
        return {
            'run_id': int(run_id),
            'program_id': int(program_id),
            'run_index': int(run_index),
            'label': f'{int(program_id)}.{int(run_index)}',
            'started_at': started_text,
            'stopped_at': stopped_text,
            'status': str(status),
            'measurement_stats': stats,
        }

    def list_program_runs(self, program_id: int) -> List[Dict[str, Any]]:
        query = """
        SELECT id, program_id, run_index, started_at, stopped_at, status
        FROM program_runs
        WHERE program_id = %s
        ORDER BY run_index DESC, id DESC;
        """
        self.db.cur.execute(query, (program_id,))
        return [self._program_run_dict(row) for row in self.db.cur.fetchall()]

    def count_program_runs(self, program_id: int) -> int:
        value = self._scalar(
            'SELECT COUNT(*) FROM program_runs WHERE program_id = %s;',
            (program_id,),
        )
        return int(value or 0)

    def program_run_counts_all(self) -> Dict[int, int]:
        query = """
        SELECT program_id, COUNT(*)
        FROM program_runs
        GROUP BY program_id;
        """
        self.db.cur.execute(query)
        return {int(program_id): int(count) for program_id, count in self.db.cur.fetchall()}

    def _program_exists(self, program_id: int) -> bool:
        value = self._scalar('SELECT COUNT(*) FROM programs WHERE ID = %s;', (program_id,))
        return bool(value)

    def add_program(self) -> int:
        query = "INSERT INTO programs (Status) VALUES ('New');"
        try:
            self.db.cur.execute(query)
            self.db.conn.commit()
            return int(self.db.cur.lastrowid)
        except Exception:
            self.db.conn.rollback()
            return 0

    def delete_program(self, program_id: int) -> int:
        query = 'DELETE FROM programs WHERE ID = %s;'
        try:
            self.db.cur.execute(query, (program_id,))
            affected = int(self.db.cur.rowcount)
            self.db.conn.commit()
            self._program_start_monotonic_ns.pop(program_id, None)
            return affected
        except Exception:
            self.db.conn.rollback()
            return 0

    def get_all_programs(self):
        query = 'SELECT ID, DateTime, Status FROM programs ORDER BY DateTime DESC, ID DESC;'
        try:
            self.db.cur.execute(query)
            return self.db.cur.fetchall()
        except Exception:
            return []

    def get_program_by_id(self, program_id: int):
        query = 'SELECT ID, DateTime, Status FROM programs WHERE ID = %s;'
        self.db.cur.execute(query, (program_id,))
        return self.db.cur.fetchone()

    def update_program_status(self, program_id: int, new_status: str) -> int:
        query = 'UPDATE programs SET Status = %s WHERE ID = %s;'
        try:
            self.db.cur.execute(query, (new_status, program_id))
            affected = int(self.db.cur.rowcount)
            self.db.conn.commit()
            return affected
        except Exception:
            self.db.conn.rollback()
            return 0

    def get_programs_by_date(self, target_date: str = '1970-01-01'):
        query = 'SELECT ID, DateTime, Status FROM programs WHERE DATE(DateTime) = %s ORDER BY ID;'
        self.db.cur.execute(query, (target_date,))
        return self.db.cur.fetchall()

    def get_program_params_temp(self, program_id: int):
        query = 'SELECT id, program_id, t_start, t_stop, minutes FROM program_temp WHERE program_id = %s ORDER BY id;'
        try:
            self.db.cur.execute(query, (program_id,))
            return self.db.cur.fetchall()
        except Exception:
            return []

    def add_e720(self, data: Dict[str, Any]) -> bool:
        config_json = json.dumps(data['config'])
        query = """
                INSERT INTO program_meta (program_id, `key`, `value`)
                VALUES
                  (%s, 'param', %s),
                  (%s, 'freq', %s)
                ON DUPLICATE KEY
                  UPDATE `value` = VALUES(`value`);
                """
        params = (data['id'], str(data['param']), data['id'], config_json)
        try:
            self.db.cur.execute(query, params)
            self.db.conn.commit()
            # rowcount is 0 when ON DUPLICATE KEY UPDATE leaves values unchanged — still success
            return True
        except Exception:
            return False

    def get_e720(self, program_id: int):
        rows = self.get_program_meta(program_id)
        if not rows:
            return {}

        result = {'id': program_id}
        for _, _, key, raw_val in rows:
            if key == 'param':
                result['param'] = int(raw_val)
            elif key == 'freq':
                result['config'] = json.loads(raw_val)
            else:
                result[key] = raw_val

        return result

    def get_program_meta(self, program_id: int):
        query = 'SELECT id, program_id, `key`, `value` FROM program_meta WHERE program_id = %s ORDER BY id;'
        try:
            self.db.cur.execute(query, (program_id,))
            return self.db.cur.fetchall()
        except Exception:
            return []

    def set_program_meta(self, program_id: int, key: str, value: str) -> bool:
        query = """
            INSERT INTO program_meta (program_id, `key`, `value`)
            VALUES (%s, %s, %s)
            ON DUPLICATE KEY UPDATE `value` = VALUES(`value`);
        """
        try:
            self.db.cur.execute(query, (int(program_id), str(key), str(value)))
            self.db.conn.commit()
            return True
        except Exception:
            self.db.conn.rollback()
            return False

    def get_program_detail(self, program_id: int) -> Dict[str, Any]:
        row = self.get_program_by_id(program_id)
        if not row:
            return {}
        item_id, dt, status = row
        meta_rows = self.get_program_meta(program_id)
        meta = {key: val for _, _, key, val in meta_rows}
        steps = [
            {'step_id': step_id, 't_start': t_start, 't_stop': t_stop, 'minutes': minutes}
            for step_id, _, t_start, t_stop, minutes in self.get_program_params_temp(program_id)
        ]
        e720 = self.get_e720(program_id)
        return {
            'id': int(item_id),
            'datetime': dt.isoformat() if hasattr(dt, 'isoformat') else str(dt),
            'status': str(status),
            'description': meta.get('description', ''),
            'meta': meta,
            'steps': steps,
            'e720': e720,
            'measurement_stats': self.get_measurement_stats(program_id),
        }

    def set_program_temp(self, data: Dict[str, Any]) -> int:
        sql = """
        INSERT INTO program_temp (program_id, t_start, t_stop, minutes)
        VALUES (%s, %s, %s, %s)
        """
        params = (
            data['program_id'],
            data['t_start'],
            data['t_stop'],
            data['minutes'],
        )
        try:
            self.db.cur.execute(sql, params)
            self.db.conn.commit()
            return int(self.db.cur.lastrowid)
        except Exception:
            self.db.conn.rollback()
            return 0

    def delete_program_temp(self, temp_id: int) -> int:
        query = 'DELETE FROM program_temp WHERE id = %s;'
        try:
            self.db.cur.execute(query, (temp_id,))
            affected = int(self.db.cur.rowcount)
            self.db.conn.commit()
            return affected
        except Exception:
            self.db.conn.rollback()
            return 0

    def update_program_temp(self, data: Dict[str, Any]) -> int:
        step_id = int(data['id'])
        program_id = int(data.get('program_id', 0) or 0)
        if program_id > 0:
            sql = """
            UPDATE program_temp
            SET t_start = %s, t_stop = %s, minutes = %s
            WHERE id = %s AND program_id = %s
            """
            params = (data['t_start'], data['t_stop'], data['minutes'], step_id, program_id)
        else:
            sql = """
            UPDATE program_temp
            SET t_start = %s, t_stop = %s, minutes = %s
            WHERE id = %s
            """
            params = (data['t_start'], data['t_stop'], data['minutes'], step_id)
        try:
            self.db.cur.execute(sql, params)
            affected = int(self.db.cur.rowcount)
            self.db.conn.commit()
            if affected > 0:
                return step_id
            check = (
                'SELECT id FROM program_temp WHERE id = %s AND program_id = %s'
                if program_id > 0
                else 'SELECT id FROM program_temp WHERE id = %s'
            )
            check_params = (step_id, program_id) if program_id > 0 else (step_id,)
            self.db.cur.execute(check, check_params)
            if self.db.cur.fetchone():
                return step_id
            return 0
        except Exception:
            self.db.conn.rollback()
            return 0

    def _latest_measurement_state_for_run(self, run_id: int) -> Optional[Tuple[float, datetime]]:
        query = """
        SELECT elapsed_s, created_at
        FROM measurements
        WHERE run_id = %s
        ORDER BY id DESC
        LIMIT 1;
        """
        self.db.cur.execute(query, (run_id,))
        row = self.db.cur.fetchone()
        if row is None:
            return None
        elapsed_s = float(row[0] or 0.0)
        created_at = row[1]
        return elapsed_s, created_at

    def _latest_measurement_state(self, program_id: int) -> Optional[Tuple[float, datetime]]:
        query = """
        SELECT elapsed_s, created_at
        FROM measurements
        WHERE program_id = %s
        ORDER BY id DESC
        LIMIT 1;
        """
        self.db.cur.execute(query, (program_id,))
        row = self.db.cur.fetchone()
        if row is None:
            return None
        elapsed_s = float(row[0] or 0.0)
        created_at = row[1]
        return elapsed_s, created_at

    def _ensure_run_elapsed_anchor(self, run_id: int) -> None:
        if run_id in self._run_start_monotonic_ns:
            return

        latest = self._latest_measurement_state_for_run(run_id)
        if latest is None:
            self._run_start_monotonic_ns[run_id] = time.monotonic_ns()
            return

        latest_elapsed_s, latest_created_at = latest
        resume_elapsed_s = latest_elapsed_s
        if isinstance(latest_created_at, datetime):
            try:
                wall_delta_s = max(0.0, (datetime.now() - latest_created_at).total_seconds())
                resume_elapsed_s += wall_delta_s
            except Exception:
                pass

        self._run_start_monotonic_ns[run_id] = time.monotonic_ns() - int(resume_elapsed_s * 1_000_000_000)

    def _ensure_program_elapsed_anchor(self, program_id: int) -> None:
        if program_id in self._program_start_monotonic_ns:
            return

        latest = self._latest_measurement_state(program_id)
        if latest is None:
            self._program_start_monotonic_ns[program_id] = time.monotonic_ns()
            return

        latest_elapsed_s, latest_created_at = latest
        resume_elapsed_s = latest_elapsed_s
        if isinstance(latest_created_at, datetime):
            try:
                wall_delta_s = max(0.0, (datetime.now() - latest_created_at).total_seconds())
                resume_elapsed_s += wall_delta_s
            except Exception:
                pass

        self._program_start_monotonic_ns[program_id] = time.monotonic_ns() - int(resume_elapsed_s * 1_000_000_000)

    def _elapsed_seconds_for_run(self, run_id: int) -> float:
        with self._elapsed_lock:
            self._ensure_run_elapsed_anchor(run_id)
            start_ns = self._run_start_monotonic_ns[run_id]
            elapsed_s = (time.monotonic_ns() - start_ns) / 1_000_000_000.0
            return max(0.0, elapsed_s)

    def _elapsed_seconds_for_program(self, program_id: int) -> float:
        with self._elapsed_lock:
            self._ensure_program_elapsed_anchor(program_id)
            start_ns = self._program_start_monotonic_ns[program_id]
            elapsed_s = (time.monotonic_ns() - start_ns) / 1_000_000_000.0
            return max(0.0, elapsed_s)

    def _sync_elapsed_anchor(self, *, run_id: int, program_id: int, elapsed_s: float) -> None:
        """Keep DB monotonic anchor aligned with client-supplied scheduler elapsed."""
        anchor_ns = time.monotonic_ns() - int(max(0.0, float(elapsed_s)) * 1_000_000_000)
        with self._elapsed_lock:
            if run_id > 0:
                self._run_start_monotonic_ns[run_id] = anchor_ns
            elif program_id > 0:
                self._program_start_monotonic_ns[program_id] = anchor_ns

    def _resolve_measurement_elapsed_s(self, data: Dict[str, Any], program_id: int, run_id: int) -> float:
        if data.get('elapsed_s') is not None:
            try:
                elapsed_s = max(0.0, float(data['elapsed_s']))
            except (TypeError, ValueError):
                elapsed_s = 0.0
            self._sync_elapsed_anchor(
                run_id=run_id,
                program_id=program_id,
                elapsed_s=elapsed_s,
            )
            return elapsed_s
        if run_id > 0:
            return self._elapsed_seconds_for_run(run_id)
        return self._elapsed_seconds_for_program(program_id)

    def _measurement_insert_params(self, data: Dict[str, Any]) -> tuple:
        program_id = self._resolve_program_id(data)
        run_id = self._resolve_run_id(data)
        elapsed_s = self._resolve_measurement_elapsed_s(data, program_id, run_id)
        sql = """
        INSERT INTO measurements (
            program_id, run_id, elapsed_s, freq, measure_ch1, measure_ch2, t_ch1, t_ch2, t_exp
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        params = (
            program_id,
            run_id if run_id > 0 else None,
            elapsed_s,
            data.get('freq'),
            data.get('measure_ch1'),
            data.get('measure_ch2'),
            data.get('t_ch1'),
            data.get('t_ch2'),
            data.get('t_exp'),
        )
        return sql, params

    def add_measurement(self, data: Dict[str, Any]) -> int:
        sql, params = self._measurement_insert_params(data)
        try:
            self.db.cur.execute(sql, params)
            self.db.conn.commit()
            return int(self.db.cur.lastrowid)
        except Exception:
            self.db.conn.rollback()
            return 0

    def add_measurement_pooled(self, data: Dict[str, Any]) -> int:
        """Insert on a pooled connection so UI read queries are not blocked."""
        sql, params = self._measurement_insert_params(data)
        conn = None
        cur = None
        try:
            conn = self.db.pool.get_connection()
            cur = conn.cursor()
            cur.execute(sql, params)
            conn.commit()
            return int(cur.lastrowid)
        except Exception as exc:
            self.db._log('error', f'[DB] Measurement insert failed: {exc}')
            if conn is not None:
                try:
                    conn.rollback()
                except Exception:
                    pass
            return 0
        finally:
            if cur is not None:
                try:
                    cur.close()
                except Exception:
                    pass
            if conn is not None:
                try:
                    conn.close()
                except Exception:
                    pass

    def add_measurements_bulk(self, rows: List[Dict[str, Any]]) -> int:
        if not rows:
            return 0

        sql = """
        INSERT INTO measurements (
            program_id, run_id, elapsed_s, freq, measure_ch1, measure_ch2, t_ch1, t_ch2, t_exp
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        params = []
        for row in rows:
            program_id = self._resolve_program_id(row)
            run_id = self._resolve_run_id(row)
            elapsed_s = self._resolve_measurement_elapsed_s(row, program_id, run_id)
            params.append(
                (
                    program_id,
                    run_id if run_id > 0 else None,
                    elapsed_s,
                    row.get('freq'),
                    row.get('measure_ch1'),
                    row.get('measure_ch2'),
                    row.get('t_ch1'),
                    row.get('t_ch2'),
                    row.get('t_exp'),
                )
            )

        conn = None
        cur = None
        try:
            conn = self.db.pool.get_connection()
            cur = conn.cursor(buffered=True)
            cur.executemany(sql, params)
            conn.commit()
            return int(cur.rowcount)
        except Exception as exc:
            if conn is not None:
                try:
                    conn.rollback()
                except Exception:
                    pass
            self.db._log('error', f'[DB] Bulk insert failed: {exc}')
            return 0
        finally:
            if cur is not None:
                try:
                    cur.close()
                except Exception:
                    pass
            if conn is not None:
                try:
                    conn.close()
                except Exception:
                    pass

    def count_measurements_for_run(self, run_id: int) -> int:
        value = self._scalar('SELECT COUNT(*) FROM measurements WHERE run_id = %s;', (run_id,))
        return int(value or 0)

    def get_run_distinct_frequencies(self, run_id: int) -> List[float]:
        query = """
        SELECT DISTINCT freq
        FROM measurements
        WHERE run_id = %s AND freq IS NOT NULL
        ORDER BY freq;
        """
        self.db.cur.execute(query, (run_id,))
        return [float(row[0]) for row in self.db.cur.fetchall()]

    def get_measurements(
        self,
        program_id: int,
        limit: int = 1000,
        *,
        run_id: int = 0,
        offset: int = 0,
    ):
        offset = max(0, int(offset))
        if run_id > 0:
            query = """
            SELECT id, program_id, run_id, elapsed_s, freq, measure_ch1, measure_ch2, t_ch1, t_ch2, t_exp, created_at
            FROM measurements
            WHERE run_id = %s
            ORDER BY id
            LIMIT %s OFFSET %s;
            """
            params: Tuple[Any, ...] = (run_id, limit, offset)
        else:
            query = """
            SELECT id, program_id, run_id, elapsed_s, freq, measure_ch1, measure_ch2, t_ch1, t_ch2, t_exp, created_at
            FROM measurements
            WHERE program_id = %s
            ORDER BY id
            LIMIT %s OFFSET %s;
            """
            params = (program_id, limit, offset)
        try:
            self.db.cur.execute(query, params)
            return self.db.cur.fetchall()
        except Exception:
            return []

    def delete_measurements(self, program_id: int) -> int:
        query = 'DELETE FROM measurements WHERE program_id = %s;'
        try:
            self.db.cur.execute(query, (program_id,))
            affected = int(self.db.cur.rowcount)
            self.db.conn.commit()
            self._program_start_monotonic_ns.pop(program_id, None)
            return affected
        except Exception:
            self.db.conn.rollback()
            return 0

    def get_measurement_stats(self, program_id: int = 0, *, run_id: int = 0) -> Dict[str, Any]:
        if run_id > 0:
            query = """
            SELECT COUNT(*), MIN(elapsed_s), MAX(elapsed_s), MIN(t_ch1), MAX(t_ch1), MIN(t_ch2), MAX(t_ch2)
            FROM measurements
            WHERE run_id = %s;
            """
            self.db.cur.execute(query, (run_id,))
        else:
            query = """
            SELECT COUNT(*), MIN(elapsed_s), MAX(elapsed_s), MIN(t_ch1), MAX(t_ch1), MIN(t_ch2), MAX(t_ch2)
            FROM measurements
            WHERE program_id = %s;
            """
            self.db.cur.execute(query, (program_id,))
        row = self.db.cur.fetchone()
        if row is None:
            return {}
        return {
            'count': int(row[0] or 0),
            'elapsed_s_min': row[1],
            'elapsed_s_max': row[2],
            't_ch1_min': row[3],
            't_ch1_max': row[4],
            't_ch2_min': row[5],
            't_ch2_max': row[6],
        }
