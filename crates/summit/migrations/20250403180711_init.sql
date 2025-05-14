-- Add migration script here
CREATE TABLE IF NOT EXISTS project (
    project_id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    slug TEXT NOT NULL UNIQUE,
    summary TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS profile (
    profile_id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    arch TEXT NOT NULL,
    index_uri TEXT NOT NULL,
    status TEXT NOT NULL,
    project_id INT NOT NULL,
    UNIQUE (project_id, name),
    UNIQUE (project_id, index_uri),
    FOREIGN KEY (project_id) REFERENCES project (project_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS profile_remote (
    profile_id INT NOT NULL,
    index_uri TEXT NOT NULL,
    name TEXT NOT NULL,
    priority UNSIGNED BIG INT NOT NULL,
    PRIMARY KEY (profile_id, index_uri),
    FOREIGN KEY (profile_id) REFERENCES profile (profile_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS repository (
    repository_id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    summary TEXT NOT NULL,
    description TEXT,
    commit_ref TEXT,
    origin_uri TEXT NOT NULL,
    branch TEXT,
    status TEXT NOT NULL,
    project_id INT NOT NULL,
    UNIQUE (project_id, name),
    UNIQUE (project_id, origin_uri),
    FOREIGN KEY (project_id) REFERENCES project (project_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS task (
    task_id INTEGER PRIMARY KEY AUTOINCREMENT,
    project_id INT NOT NULL,
    profile_id INT NOT NULL,
    repository_id INT NOT NULL,
    slug TEXT NOT NULL,
    package_id TEXT NOT NULL,
    arch TEXT NOT NULL,
    build_id TEXT NOT NULL UNIQUE,
    description TEXT NOT NULL,
    commit_ref TEXT NOT NULL,
    source_path TEXT NOT NULL,
    status TEXT NOT NULL,
    allocated_builder TEXT,
    log_path TEXT,
    added BIGINT NOT NULL DEFAULT (unixepoch ()),
    started BIGINT,
    updated BIGINT NOT NULL DEFAULT (unixepoch ()),
    ended BIGINT,
    FOREIGN KEY (project_id) REFERENCES project (project_id) ON DELETE CASCADE,
    FOREIGN KEY (profile_id) REFERENCES profile (profile_id) ON DELETE CASCADE,
    FOREIGN KEY (repository_id) REFERENCES repository (repository_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS task_blockers (
    task_id INT NOT NULL,
    blocker TEXT NOT NULL,
    PRIMARY KEY (task_id, blocker),
    FOREIGN KEY (task_id) REFERENCES task (task_id) ON DELETE CASCADE
);
