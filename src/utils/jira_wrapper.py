from __future__ import annotations
from jira import JIRA
from jira.exceptions import JIRAError
from utils.config_wrapper import ConfigWrapper

class JiraWrapper(ConfigWrapper):
    jira_manager = None
    jira_user = None
    def __init__(self):
        super().__init__()

        try:
            self.jira_manager = JIRA(
                server=self.getenv("Jira_URL"),
                basic_auth=(
                    self.getenv("Email_id"), self.getenv("Jira_Api_Token")
                )
            )
            self.jira_user = self.jira_manager.current_user()
        except JIRAError as e:
            print(f"JIRA connection failed: {e}")

    def log_task(self, tsummary='', tdescription='', issue_type='Bug'):
        if self.jira_manager is None:
            return 'JIRA Manager is not initiated. Kindly provide the required access.'
        try:
            new_task = self.jira_manager.create_issue(
                project=self.getenv('Jira_Project'),
                summary=tsummary,
                description=tdescription,
                issue_type={'name':issue_type}
            )
            return f'JIRA task has been created: [{new_task.key}]'
        except Exception as e:
            return 'Error in create task' + e

    def __str__(self):
        return str( self.jira_user )

if __name__ == "__main__":
    j = JiraWrapper()
    print( j )
    print( j.log_task(tsummary='Creating issue', tdescription='Description goes here', issue_type='task'))
