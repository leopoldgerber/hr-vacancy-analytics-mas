.PHONY: init-tables
init-tables:
	python -m hr_vacancy_analytics.db.init_tables

.PHONY: api
api:
	uvicorn hr_vacancy_analytics.main:app --reload