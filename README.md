# drrqueue

纯 Python 标准库的本机服务。

## 调度语义（DRR）

- `enqueue(flow, id, size)` 入各流自己的队列；`set_weight(flow, weight)` 改每轮字节配额；
- `dequeue(rounds)` 按轮推进：每轮按流首次入队顺序给每个流加当前权重个字节的欠账，
  欠账够付队头包就出队（可连续多个），不够则欠账留到下一轮，不清零；
- 权重变化从下一轮起生效，历史欠账保留；未显式设权的流不限速（兼容旧行为）；
- 队列、权重、欠账经 `drrqueue.wal` 落盘，`POST /recover` 重放恢复，尾部半条记录忽略。

## 起服务

    python3 server.py 8000

浏览器打开 http://127.0.0.1:8000/ 看结果。

## 测试

    python3 -m unittest discover -s tests -v

## 验收自检

    python3 check_http.py
