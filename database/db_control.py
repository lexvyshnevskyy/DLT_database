from __future__ import annotations

import json
import time
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Tuple

from .connector import DbConnector


class DbControl:
    def __init__(self, connector: DbConnector):
        self.db = connector
        self._program_start_monotonic_ns: Dict[int, int] = {}

    def _scalar(self, query: str, params: Iterable[Any]) -> Optional[Any]:
        self.db.cur.execute(query, tuple(params))
        row = self.db.cur.fetchone()
        if row is None:
            return None
        return row[0]

    def _resolve_program_id(self, data: Dict[str, Any]) -> int:
        raw_value = data.get('program_id', data.get('exp_id', 0))
        return int(raw_value)

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
            return self.db.cur.rowcount > 0
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
        sql = """
        UPDATE program_temp
        SET
            t_start = %s,
            t_stop  = %s,
            minutes = %s
        WHERE id = %s
        """
        params = (
            data['t_start'],
            data['t_stop'],
            data['minutes'],
            data['id'],
        )
        try:
            self.db.cur.execute(sql, params)
            self.db.conn.commit()
            return int(self.db.cur.rowcount)
        except Exception:
            self.db.conn.rollback()
            return 0

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

    def _elapsed_seconds_for_program(self, program_id: int) -> float:
        self._ensure_program_elapsed_anchor(program_id)
        start_ns = self._program_start_monotonic_ns[program_id]
        elapsed_s = (time.monotonic_ns() - start_ns) / 1_000_000_000.0
        return max(0.0, elapsed_s)

    def add_measurement(self, data: Dict[str, Any]) -> int:
        program_id = self._resolve_program_id(data)
        sql = """
        INSERT INTO measurements (
            program_id, elapsed_s, freq, measure_ch1, measure_ch2, t_ch1, t_ch2, t_exp
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        params = (
            program_id,
            self._elapsed_seconds_for_program(program_id),
            data.get('freq'),
            data.get('measure_ch1'),
            data.get('measure_ch2'),
            data.get('t_ch1'),
            data.get('t_ch2'),
            data.get('t_exp'),
        )
        try:
            self.db.cur.execute(sql, params)
            self.db.conn.commit()
            return int(self.db.cur.lastrowid)
        except Exception:
            self.db.conn.rollback()
            return 0

    def add_measurements_bulk(self, rows: List[Dict[str, Any]]) -> int:
        if not rows:
            return 0

        sql = """
        INSERT INTO measurements (
            program_id, elapsed_s, freq, measure_ch1, measure_ch2, t_ch1, t_ch2, t_exp
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        params = []
        for row in rows:
            program_id = self._resolve_program_id(row)
            params.append(
                (
                    program_id,
                    self._elapsed_seconds_for_program(program_id),
                    row.get('freq'),
                    row.get('measure_ch1'),
                    row.get('measure_ch2'),
                    row.get('t_ch1'),
                    row.get('t_ch2'),
                    row.get('t_exp'),
                )
            )
        try:
            self.db.cur.executemany(sql, params)
            self.db.conn.commit()
            return int(self.db.cur.rowcount)
        except Exception:
            self.db.conn.rollback()
            return 0

    def get_measurements(self, program_id: int, limit: int = 1000):
        query = """
        SELECT id, program_id, elapsed_s, freq, measure_ch1, measure_ch2, t_ch1, t_ch2, t_exp, created_at
        FROM measurements
        WHERE program_id = %s
        ORDER BY id
        LIMIT %s;
        """
        try:
            self.db.cur.execute(query, (program_id, limit))
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

    def get_measurement_stats(self, program_id: int) -> Dict[str, Any]:
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
