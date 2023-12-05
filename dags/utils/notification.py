import os
from airflow.providers.telegram.hooks.telegram import TelegramHook

class Notification:
    """
    A class that handles notifications for task instances in Airflow.
    """

    def __init__(self, context):
        """
        Initializes a Notification object.

        Args:
            context (dict): The context dictionary containing information about the task instance.
        """
        self.context = context
        self.subject = self.get_subject()

    def get_subject(self):
        """
        Generates the subject for the notification email based on the task instance state.

        Returns:
            str: The subject of the notification email.
        """
        state = self.context['task_instance'].state
        task_instance_key_str = self.context['task_instance_key_str']
        duration = self.context['task_instance'].duration


        dag_name = task_instance_key_str.split('__')[0]
        task_name = task_instance_key_str.split('__')[1]

        if state == 'success':
            return f"Airflow Run Success! ✅ \nDAG: {dag_name} \nTask Name: {task_name} \nDuration {duration}"
        elif state == 'up_for_retry':
            return f"Airflow Run Retry! ⚠️ \nDAG: {dag_name} \nTask Name: {task_name} \nDuration {duration}"
        else:
            return f"Airflow Run Failure! ⛔ \nDAG: {dag_name} \nTask Name: {task_name} \nDuration {duration}"

    def send_telegram(self):
        """
        Sends the notification message to Telegram.

        Returns:
            str: The result of sending the Telegram message.
        """
        telegram_token = os.getenv('TELEGRAM_TOKEN')
        telegram_chat_id = os.getenv('TELEGRAM_CHAT_ID')
        telegram = TelegramHook(token=telegram_token, chat_id=telegram_chat_id)
        return telegram.send_message({'text': self.subject, 'parse_mode': 'HTML'})

    @staticmethod
    def push(context):
        """
        Sends the notification email and Telegram message.

        Args:
            context (dict): The context dictionary containing information about the task instance.
        """
        notification = Notification(context)
        notification.send_telegram()
