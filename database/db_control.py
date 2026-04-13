from __future__ import annotations

import json
from typing import Any, Dict

from .connector import DbConnector


class DbControl:
    def __init__(self, connector: DbConnector):
        self.db = connector

    def add_program(self) -> int:
        query = "INSERT INTO programs (Status) VALUES ('New');"
        try:
            self.db.cur.execute(query)
            self.db.conn.commit()
            return int(self.db.cur.lastrowid)
        except Exception:
            return 0

    def delete_program(self, program_id: int) -> int:
        query = "DELETE FROM programs WHERE ID = %s;"
        self.db.cur.execute(query, (program_id,))
        affected = int(self.db.cur.rowcount)
        self.db.conn.commit()
        return affected

    def get_all_programs(self):
        query = "SELECT * FROM programs;"
        try:
            self.db.cur.execute(query)
            return self.db.cur.fetchall()
        except Exception as exc:
            print("exception", exc)
            return []

    def get_program_by_id(self, program_id: int):
        query = "SELECT * FROM programs WHERE ID = %s;"
        self.db.cur.execute(query, (program_id,))
        return self.db.cur.fetchone()

    def update_program_status(self, program_id: int, new_status: str) -> None:
        query = "UPDATE programs SET Status = %s WHERE ID = %s;"
        self.db.cur.execute(query, (new_status, program_id))
        self.db.conn.commit()

    def get_programs_by_date(self, target_date: str = "1970-01-01"):
        query = "SELECT * FROM programs WHERE DATE(DateTime) = %s;"
        self.db.cur.execute(query, (target_date,))
        return self.db.cur.fetchall()

    def get_program_params_temp(self, program_id: int):
        query = "SELECT * FROM program_temp WHERE program_id = %s ORDER BY id;"
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

        result = {"id": program_id}
        for _, _, key, raw_val in rows:
            if key == "param":
                result["param"] = int(raw_val)
            elif key == "freq":
                result["config"] = json.loads(raw_val)
            else:
                result[key] = raw_val

        return result

    def get_program_meta(self, program_id: int):
        query = "SELECT * FROM program_meta WHERE program_id = %s;"
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
            data["program_id"],
            data["t_start"],
            data["t_stop"],
            data["minutes"],
        )

        self.db.cur.execute(sql, params)
        self.db.conn.commit()
        return int(self.db.cur.lastrowid)

    def delete_program_temp(self, temp_id: int) -> int:
        query = "DELETE FROM program_temp WHERE id = %s;"
        try:
            self.db.cur.execute(query, (temp_id,))
            affected = int(self.db.cur.rowcount)
            self.db.conn.commit()
        except Exception:
            self.db.conn.rollback()
            return 0
        return affected

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
            data["t_start"],
            data["t_stop"],
            data["minutes"],
            data["id"],
        )

        self.db.cur.execute(sql, params)
        self.db.conn.commit()
        return int(self.db.cur.rowcount)
