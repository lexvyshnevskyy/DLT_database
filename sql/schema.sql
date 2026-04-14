CREATE TABLE IF NOT EXISTS programs (
    ID INT NOT NULL AUTO_INCREMENT,
    DateTime DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    Status VARCHAR(32) NOT NULL DEFAULT 'New',
    PRIMARY KEY (ID)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS program_temp (
    id INT NOT NULL AUTO_INCREMENT,
    program_id INT NOT NULL,
    t_start DOUBLE NOT NULL,
    t_stop DOUBLE NOT NULL,
    minutes DOUBLE NOT NULL,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    KEY idx_program_temp_program_id (program_id),
    CONSTRAINT fk_program_temp_program
      FOREIGN KEY (program_id) REFERENCES programs(ID)
      ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS program_meta (
    id INT NOT NULL AUTO_INCREMENT,
    program_id INT NOT NULL,
    `key` VARCHAR(64) NOT NULL,
    `value` LONGTEXT NULL,
    PRIMARY KEY (id),
    UNIQUE KEY uk_program_meta_program_key (program_id, `key`),
    KEY idx_program_meta_program_id (program_id),
    CONSTRAINT fk_program_meta_program
      FOREIGN KEY (program_id) REFERENCES programs(ID)
      ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS measurements (
    id BIGINT NOT NULL AUTO_INCREMENT,
    program_id INT NOT NULL,
    elapsed_s DOUBLE NOT NULL DEFAULT 0,
    freq DOUBLE NULL,
    measure_ch1 DOUBLE NULL,
    measure_ch2 DOUBLE NULL,
    t_ch1 DOUBLE NULL,
    t_ch2 DOUBLE NULL,
    t_exp DOUBLE NULL,
    created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    PRIMARY KEY (id),
    KEY idx_measurements_program_id (program_id),
    KEY idx_measurements_elapsed_s (elapsed_s),
    CONSTRAINT fk_measurements_program
      FOREIGN KEY (program_id) REFERENCES programs(ID)
      ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
