# drrqueue

纯 Python 标准库的本机服务。

## 起服务

    python3 server.py 8000

浏览器打开 http://127.0.0.1:8000/ 看结果。

## 测试

    python3 -m unittest discover -s tests -v

## 验收自检

    python3 check_http.py

## 持久化

enqueue/dequeue/set_weight 以 JSON 行追加到 `drrqueue.wal`；
`POST /recover` 重放 WAL 恢复队列、权重、欠账，尾部半条记录自动忽略。
服务每次启动从空队列开始（删除旧 WAL）。
