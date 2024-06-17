"""
"""

from record import Record
import os

class ModelTrainingRunJob(Record):
    """
    """
    def __init__(self, id=None, **kwargs):
        """
        """
        kwargs.pop('object_type', None) #remove object type from kwargs, as it is set in super().__init__
        super().__init__(id, object_type='model_training_run_job', **kwargs)
        self.config = self.kwargs.get('config', {})
        self.market_run = None
        self.region = None
        self.s_date = None
        self.e_date = None
        self._status = 'created'
        self._worker_ip = None
        self.created_at = None
        self._task_id = None
        self.setup_mode = False
        self.parent_id = None
        self.setup_mode = False
        self._setup()
    
    @property
    def status(self):
        """
        """
        return self._status
    
    @status.setter
    def status(self, value):
        """
        """
        if value not in ['created', 'running', 'complete', 'failed']:
            raise ValueError(f"Invalid status {value}")
        self._status = value
        if not self.setup_mode:
            self.add_keys(status=value)

    @property
    def task_id(self):
        """
        """
        return self._task_id
    
    @task_id.setter
    def task_id(self, value):
        """set Celery task id
        """
        self._task_id = value
        if not self.setup_mode:
            self.add_keys(task_id=value)
    
    @property
    def worker_ip(self):
        """
        """
        return self._worker_ip
    
    @worker_ip.setter
    def worker_ip(self, value):
        """
        """
        self._worker_ip = value
        if not self.setup_mode:
            self.add_keys(worker_ip=value)
    
    def _setup(self):
        """
        """
        if len(self.kwargs) > 2: #try dict loading first, as it is faster
            self._from_dict(self.kwargs)
        elif 'config' in self.kwargs:
            self.config = self.kwargs['config']
            self._from_config()
        elif self._id is not None:
            self._from_id()
    
    def _from_config(self) -> None:
        """load existing args from config
        """
        self.setup_mode = True
        for (k, v) in self.config.items():
            if hasattr(self, k):
                setattr(self, k, v)
        self.setup_mode = False
    
    def to_dict(self):
        """
        """
        return {
            'config': self.config,
            'market_run': self.market_run,
            'region': self.region,
            's_date': self.s_date,
            'e_date': self.e_date,
            'status': self.status,
            'worker_ip': self.worker_ip,
            'task_id': self.task_id,
            'object_type': 'model_training_run_job',
            'created_at': self.created_at,
            'parent_id': self.parent_id,
        }
