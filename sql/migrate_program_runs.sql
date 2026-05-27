CREATE TABLE IF NOT EXISTS program_runs (
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
