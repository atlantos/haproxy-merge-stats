# haproxy-merge-stats
Script to consolidate and merge stats from HAProxy multi process backends. Useful to gather stats with HAProxy Prometheus Exporter, etc. Requires Python 2.

## Usage
* Update haproxy.cfg to bind stats sockets to individual HAProxy processes:
```nbproc 4
stats socket /var/lib/haproxy/stats1 mode 644 level operator process 1
stats socket /var/lib/haproxy/stats2 mode 644 level operator process 2
stats socket /var/lib/haproxy/stats3 mode 644 level operator process 3
stats socket /var/lib/haproxy/stats4 mode 644 level operator process 4
```
* Start haproxy_merge_stats
```python haproxy_merge_stats.py /var/lib/haproxy/stats /var/lib/haproxy/stats1 /var/lib/haproxy/stats2 /var/lib/haproxy/stats3 /var/lib/haproxy/stats4```
* Use `/var/lib/haproxy/stats` socket to access stats. For example with HAProxy Prometheus Exporter
```haproxy_exporter --haproxy.scrape-uri=unix:/var/lib/haproxy/stats```
