#! -*- coding: utf-8 -*-
#
# author: forcemain@163.com

from __future__ import annotations

import typing as t

from logging import getLogger
from timeit import default_timer
from prometheus_client import Gauge
from prometheus_client import Counter
from prometheus_client import Histogram
from service_core.core.context import WorkerContext
from service_core.core.service.dependency import Dependency

logger = getLogger(__name__)


class Prometheus(Dependency):
    """ Prometheus依赖类

    doc: https://github.com/prometheus/client_python
    """

    name = 'Prometheus'

    def __init__(self, alias: t.Text, **kwargs: t.Any) -> None:
        """ 初始化实例

        @param alias: 配置别名
        @param kwargs: 其它配置
        """
        self.alias = alias
        self.req_current_count = None
        self.splits_thread_count = None
        self.worker_thread_count = None
        self.req_latency_seconds = None
        self.request_latency_seconds_map = {}
        # 让每次请求都调用生命周期函数来自动生成指标数据
        kwargs.setdefault('skip_callme', False)
        super(Prometheus, self).__init__(**kwargs)

    def setup(self) -> None:
        """ 声明周期 - 载入阶段 """
        self.splits_thread_count = Gauge(
            'splits_thread_count', 'splits thread count', ('host',)
        )
        self.worker_thread_count = Gauge(
            'worker_thread_count', 'worker thread count', ('host',)
        )
        self.req_current_count = Counter(
            'req_current_count', 'request current count', ('server', 'driver', 'endpoint', 'status')
        )
        self.req_latency_seconds = Histogram(
            'req_latency_seconds', 'request latency seconds', ('server', 'driver', 'endpoint', 'status')
        )

    def worker_setups(self, context: WorkerContext) -> None:
        """ 工作协程 - 载入回调

        @param context: 上下文对象
        @return: None
        """
        host = self.container.service.host
        self.splits_thread_count.labels(host).set(len(self.container.splits_threads))
        self.worker_thread_count.labels(host).set(len(self.container.worker_threads))
        self.request_latency_seconds_map.setdefault(context.worker_request_id, default_timer())

    def worker_result(self, context: WorkerContext, results: t.Any) -> None:
        """ 工作协程 - 正常回调

        @param context: 上下文对象
        @param results: 执行结果
        @return: None
        """
        self.update_request_metrics(context, status='succ')

    def worker_errors(self, context: WorkerContext, excinfo: t.Any) -> None:
        """ 工作协程 - 异常回调

        @param context: 上下文对象
        @param excinfo: 异常对象
        @return: None
        """
        self.update_request_metrics(context, status='fail')

    def update_request_metrics(self, context: WorkerContext, status: t.Text):
        """ 更新请求相关的指标

        :param context: 上下文对象
        :param status: 执行结果
        :return: None
        """
        server = self.container.service.host
        driver = context.original_entrypoint.name
        request_id = context.worker_request_id
        endpoint = context.original_entrypoint.object_name
        self.req_latency_seconds.labels(
            server, driver, endpoint, status
        ).observe(
            default_timer() - self.request_latency_seconds_map.pop(request_id)
        )
        self.req_current_count.labels(server, driver, endpoint, status).inc()
