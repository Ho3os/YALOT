from queue import Queue
import uuid





class TaskManager():
    def __init__(self, general_handlers):
        self.stratagy_coordinator = general_handlers["strategy_coordinator"]
        self.scope = general_handlers["scope"]
        self.queue_tsk = TaskQueue()

    def create_task_message(self, operation="search", source_type="based_on_scope", modules='', source=''):
        task_message = {
            "id": str(uuid.uuid4()),
            "operation": operation,
            "source_type": source_type,
            "module_name": modules,
            "source": source,
        }
        return task_message
    
    def execute_workflow_search_based_scope(self, default):
        self.queue_tsk.put_task(self.create_task_message("search","based_on_scope",'columbus'))
        self.queue_tsk.put_task(self.create_task_message("search","based_on_scope",'crtsh'))
        self.queue_tsk.put_task(self.create_task_message("search","based_on_scope",'whoisxml_subdomains'))
        self.queue_tsk.put_task(self.create_task_message("search","based_on_output",'dns_resolver', 'collection'))
        self.queue_tsk.put_task(self.create_task_message("search","based_on_scope",'shodan'))
        self.run_all_tasks()

    def execute_workflow_search_based_output(self, default):
        self.queue_tsk.put_task(self.create_task_message("search","based_on_output",'columbus', 'collection'))
        self.queue_tsk.put_task(self.create_task_message("search","based_on_output",'crtsh', 'collection'))
        self.queue_tsk.put_task(self.create_task_message("search","based_on_output",'dns_resolver', 'collection'))
        self.queue_tsk.put_task(self.create_task_message("search","based_on_output",'whoisxml_subdomains', 'collection'))
        self.queue_tsk.put_task(self.create_task_message("search","based_on_output",'shodan', 'collection'))
        self.run_all_tasks()

    def execute_workflow_merge_all(self, default):
        self.queue_tsk.put_task(self.create_task_message("merge","all",'collection'))
        self.run_all_tasks()

    def execute_workflow_merge(self, default):
        self.queue_tsk.put_task(self.create_task_message("merge","from_input_source",'collection', 'columbus'))
        self.queue_tsk.put_task(self.create_task_message("merge","from_input_source",'collection', 'crtsh'))
        self.queue_tsk.put_task(self.create_task_message("merge","from_input_source",'collection', 'dns_resolver'))
        self.queue_tsk.put_task(self.create_task_message("merge","from_input_source",'collection', 'whoisxml_subdomains'))
        self.queue_tsk.put_task(self.create_task_message("merge","from_input_source",'collection', 'shodan'))
        self.run_all_tasks()


    def execute_manual(self,default):
        #self.queue_tsk.put_task(self.create_task_message("search","based_on_output",'dns_resolver', 'collection'))
        #self.queue_tsk.put_task(self.create_task_message("merge","from_input_source",'collection', 'dns_resolver'))
        #self.queue_tsk.put_task(self.create_task_message("search","based_on_output",'shodan', 'collection'))
        self.queue_tsk.put_task(self.create_task_message("merge","from_input_source",'collection', 'shodan'))
        self.queue_tsk.put_task(self.create_task_message("scope","publish_update",'all'))
        self.run_all_tasks()

    def execute_scope_update_all(self, message=''):
        self.queue_tsk.put_task(self.create_task_message("scope","publish_update",'all'))
        self.run_all_tasks()
        

    def run_all_tasks(self):
        while True:
            task = self.queue_tsk.get_next_task()
            if task == {"message": "No tasks in the queue"}:
                print("All tasks have been processed.")
                break
            else:
                print(f"Processing task: {task}")
                self.execute_task_message(task)


    def update_task_message(self, name, modules, strategy_name):
        pass

    def execute_task_message(self,msg):
        if self.validate_message_format(msg):
            if msg["operation"] == "search" or msg["operation"] == "merge":
                self.stratagy_coordinator.execute_new_task(msg)
            elif  msg["operation"] == "scope":
                self.scope.publish_update_scope("TaskManager")


    def validate_message_format(self,msg):
        if "source_type" not in msg:
            raise ValueError(f"Source Type is not part of the task message")
        if "operation" not in msg:
            raise ValueError(f"Operation is not part of the task message")
        if "module_name" not in msg:
            raise ValueError(f"modules is not part of the task message")
        return True




class TaskQueue():
    def __init__(self):
        self.queue = Queue()

    def put_task(self, task_message):
        self.queue.put(task_message)

    def get_next_task(self):
        if self.queue.empty():
            return {"message": "No tasks in the queue"}
        return self.queue.get()