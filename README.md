# Tamara Data Engineer Coding Challenge

## Requirements
Our data warehouse system was replicated from our application data (in real time). The payload for order events in our application is in json format which didn't optimize for analytics applications. So when we need query/aggregate fields in a JSON column, it will take a considerable time. And we can't add indexes to fields inside the JSON column to speed up the query.

So we need to create some pipelines to denormalize JSON fields in order_events tables. The new data structure should answer business questions by conducting simple queries:

- Top 10 most purchased items by day, month, quarter, year.
- Top 10 items that contributed most to the late fee.
- Top 10 merchants who have most new order value by day, month, quarter, year
- Top 10 merchants who have most canceled order value by day, month, quarter, year
- Total late fee amount collected by day, month, quarter, year.

---
### Acceptance Criteria
- Setup project structure + Docker + code linting + mypy
- Storage:
Input: MySQL
Output: Mysql
- Answer all critical business questions

### Good to have:
- Pipeline that support incremental update in real time
- Unit tests and integration tests for critical logic
- Architecture: Cloud native (K8s)
- Define CICD (github, gitlab,...)

