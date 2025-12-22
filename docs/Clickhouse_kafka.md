Event Simulation for Job Pricing (ClickHouse + Kafka)

This script simulates event-level logs (impression/view/apply/hire) from offline training samples.

Purpose:
Bridge offline training data and online event stream
Ensure feature-time alignment (24h before label_time)
Provide mock Kafka input for ClickHouse ingestion

Files:
make_events.py: generate event-level job logs
Input: train_samples.csv
Output: job_events.csv

Event Schema:
event_time
event_type (impression / view / apply / hire)
job_id
company_id
user_id (simulated)


