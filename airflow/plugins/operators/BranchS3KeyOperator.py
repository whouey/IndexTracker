from airflow.operators.branch import BaseBranchOperator
from typing import Dict, Iterable, Union

class BranchS3KeyOperator(BaseBranchOperator):
    """
    THIS CLASS IS UNDER DEVELOPING.
    """

    def choose_branch(self, context: Dict) -> Union[str, Iterable[str]]:
        """
        Subclasses should implement this, running whatever logic is
        necessary to choose a branch and returning a task_id or list of
        task_ids.

        :param context: Context dictionary as passed to execute()
        :type context: dict
        """
        raise NotImplementedError