DROP TABLE IF EXISTS vacancy_activity;
CREATE TABLE IF NOT EXISTS vacancy_activity (
    id SERIAL PRIMARY KEY,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),

    client_id INTEGER NOT NULL,
    source TEXT,
    vacancy_id BIGINT NOT NULL,
    publication_date TIMESTAMP,
    tariff TEXT,

    responses INTEGER,
    total_responses INTEGER,

    company_name TEXT,

    salary_from_recalculated INTEGER,
    salary_to_recalculated INTEGER,
    tax NUMERIC(12, 4),
    salary_indication TEXT,

    city TEXT,
    profile TEXT,
    region TEXT,

    employment_type TEXT,
    work_experience TEXT,
    work_schedule TEXT,

    date DATE,

    vacancy_title TEXT,

    salary_from INTEGER,
    salary_to INTEGER,
    payment_type TEXT,

    specialization TEXT,
    skills TEXT,
    metro_stations TEXT,
    vacancy_description TEXT,

    config_id TEXT
);

-- Indexes (BTREE by default)
CREATE INDEX IF NOT EXISTS idx_vacancy_activity_client_id
    ON vacancy_activity (client_id);

CREATE INDEX IF NOT EXISTS idx_vacancy_activity_vacancy_id
    ON vacancy_activity (vacancy_id);

CREATE INDEX IF NOT EXISTS idx_vacancy_activity_city
    ON vacancy_activity (city);

CREATE INDEX IF NOT EXISTS idx_vacancy_activity_profile
    ON vacancy_activity (profile);
