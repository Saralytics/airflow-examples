from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator


class ReportPodOperator(KubernetesPodOperator):

    def __init__(self, dag, name, task_id, args, cmds=None, **kwargs):
        super().__init__(
            dag=dag,
            namespace='airflow',
            image='339765050942.dkr.ecr.eu-west-1.amazonaws.com/sagemaker/cochlea:v2',
            annotations={
                "iam.amazonaws.com/role": "arn:aws:iam::339765050942:role/service-role/AmazonSageMaker-ExecutionRole-20191107T161513"
            },
            cmds=cmds,
            arguments=args,
            name=name,
            task_id=task_id,
            **kwargs
        )