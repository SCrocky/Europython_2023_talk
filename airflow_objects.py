from airflow.models.baseoperator import BaseOperator
from airflow.hooks.base import BaseHook
from airflow.exceptions import AirflowException
import praw


class RedditHook(BaseHook):
    """
    Retrieves a reddit [`praw.reddit.Reddit`](https://praw.readthedocs.io/en/stable/) session
    """
    @staticmethod
    def get_conn(conn_id=None):
        if conn_id is None:
            raise AirflowException("Argument `conn_id` was not passed")

        conn = BaseHook.get_connection(conn_id)
        return praw.Reddit(
            username=conn.login,
            password=conn.password,
            user_agent=conn.extra_dejson["user_agent"],
            client_id=conn.extra_dejson["client_id"],
            client_secret=conn.extra_dejson["client_secret"],
        )


class RedditMessageOperator(BaseOperator):
    """
    Sends a reddit message to the specified reddit user
    """
    def __init__(
        self,
        *,
        reddit_conn,
        target_user,
        message,
        **kwargs
    ):
        super().__init__(**kwargs)
        self.conn = reddit_conn
        self.target_user = target_user
        self.message = message

    def execute(self, context):
        reddit = RedditHook.get_conn(self.conn)
        redditor = praw.models.Redditor(reddit, self.target_user)
        redditor.message(subject=f"Automated {context['dag'].dag_id} message", message=self.message)


