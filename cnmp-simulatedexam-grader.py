import boto3
import json
import logging
from kubernetes import client, config

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Load kubectl configurations
try:
    config.load_kube_config()
    logging.info("Kubernetes configuration loaded successfully")
except Exception as e:
    logging.error("Failed to load kube config: %s", str(e))
    raise

# Setup SQS Client
sqs = boto3.client('sqs')
input_queue_url = 'https://sqs.ap-southeast-1.amazonaws.com/010438472482/gcash-fecp-autograder-request'
output_queue_url = 'https://sqs.ap-southeast-1.amazonaws.com/010438472482/gcash-fecp-autograder-response'

# Kubernetes API Clients
v1 = client.CoreV1Api()
batch_v1 = client.BatchV1Api()
apps_v1 = client.AppsV1Api()

# Task Weights (as percentages, summing to 100%)
task_weights = {
    "task_01": 0.05,  # 5%
    "task_02": 0.06,  # 6%
    "task_03": 0.06,  # 6%
    "task_04": 0.06,  # 6%
    "task_05": 0.05,  # 5%
    "task_06": 0.06,  # 6%
    "task_07": 0.05,  # 5%
    "task_08": 0.06,  # 6%
    "task_09": 0.05,  # 5%
    "task_10": 0.05,  # 5%
    "task_11": 0.16,  # 16%
    "task_12": 0.06,  # 6%
    "task_13": 0.06,  # 6%
    "task_14": 0.06,  # 6%
    "task_15": 0.07,  # 7%
}

# Store grades per user (email) in memory
user_grades = {}  # Format: {email: {task_id: grade, ...}}

# Task Verification Functions (unchanged from original)
def check_task_01(email, submission):
    namespace = submission.get('namespace', 'default')
    try:
        pod = v1.read_namespaced_pod(name="nginx-pod", namespace=namespace)
        if pod.spec.containers[0].image != "nginx:1.14.2":
            logging.error("Pod nginx-pod in %s has incorrect image", namespace)
            return False
        if not any(port.container_port == 80 for port in pod.spec.containers[0].ports):
            logging.error("Pod nginx-pod in %s does not expose port 80", namespace)
            return False
        logging.info("Pod nginx-pod in %s is correctly configured", namespace)
        return True
    except client.exceptions.ApiException as e:
        logging.error("Failed to check nginx-pod in %s: %s", namespace, str(e))
        return False

def check_task_02(email, submission):
    namespace = submission.get('namespace', 'app')
    try:
        pod = v1.read_namespaced_pod(name="multi-pod", namespace=namespace)
        containers = {c.name: c for c in pod.spec.containers}
        if len(containers) != 2:
            logging.error("Pod multi-pod in %s does not have two containers", namespace)
            return False
        if containers.get("main").image != "busybox" or containers.get("main").command != ["sleep", "3600"]:
            logging.error("Pod multi-pod main container in %s is misconfigured", namespace)
            return False
        if containers.get("sidecar").image != "redis":
            logging.error("Pod multi-pod sidecar container in %s is misconfigured", namespace)
            return False
        logging.info("Pod multi-pod in %s is correctly configured", namespace)
        return True
    except client.exceptions.ApiException as e:
        logging.error("Failed to check multi-pod in %s: %s", namespace, str(e))
        return False

def check_task_03(email, submission):
    namespace = submission.get('namespace', 'default')
    try:
        deployment = apps_v1.read_namespaced_deployment(name="webapp", namespace=namespace)
        if deployment.spec.replicas != 3:
            logging.error("Deployment webapp in %s has incorrect replicas", namespace)
            return False
        if deployment.spec.template.spec.containers[0].image != "nginx:1.15":
            logging.error("Deployment webapp in %s has incorrect image", namespace)
            return False
        if not any(port.container_port == 80 for port in deployment.spec.template.spec.containers[0].ports):
            logging.error("Deployment webapp in %s does not expose port 80", namespace)
            return False
        if deployment.spec.selector.match_labels.get("app") != "webapp":
            logging.error("Deployment webapp in %s has incorrect selector labels", namespace)
            return False
        logging.info("Deployment webapp in %s is correctly configured", namespace)
        return True
    except client.exceptions.ApiException as e:
        logging.error("Failed to check webapp deployment in %s: %s", namespace, str(e))
        return False

def check_task_04(email, submission):
    namespace = submission.get('namespace', 'default')
    try:
        service = v1.read_namespaced_service(name="webapp-service", namespace=namespace)
        if service.spec.type != "ClusterIP":
            logging.error("Service webapp-service in %s is not ClusterIP", namespace)
            return False
        if service.spec.selector.get("app") != "webapp":
            logging.error("Service webapp-service in %s has incorrect selector", namespace)
            return False
        if not any(port.port == 80 and port.target_port == 80 for port in service.spec.ports):
            logging.error("Service webapp-service in %s has incorrect port configuration", namespace)
            return False
        logging.info("Service webapp-service in %s is correctly configured", namespace)
        return True
    except client.exceptions.ApiException as e:
        logging.error("Failed to check webapp-service in %s: %s", namespace, str(e))
        return False

def check_task_05(email, submission):
    namespace = submission.get('namespace', 'config')
    try:
        configmap = v1.read_namespaced_config_map(name="app-config", namespace=namespace)
        if configmap.data.get("db.host") != "localhost" or configmap.data.get("app.mode") != "production":
            logging.error("ConfigMap app-config in %s has incorrect data", namespace)
            return False
        logging.info("ConfigMap app-config in %s is correctly configured", namespace)
        return True
    except client.exceptions.ApiException as e:
        logging.error("Failed to check app-config in %s: %s", namespace, str(e))
        return False

def check_task_06(email, submission):
    namespace = submission.get('namespace', 'config')
    try:
        pod = v1.read_namespaced_pod(name="config-pod", namespace=namespace)
        if pod.spec.containers[0].image != "busybox" or pod.spec.containers[0].command != ["sleep", "3600"]:
            logging.error("Pod config-pod in %s has incorrect configuration", namespace)
            return False
        if not any(ref.config_map_ref.name == "app-config" for ref in pod.spec.containers[0].env_from):
            logging.error("Pod config-pod in %s does not use app-config ConfigMap", namespace)
            return False
        logging.info("Pod config-pod in %s is correctly configured", namespace)
        return True
    except client.exceptions.ApiException as e:
        logging.error("Failed to check config-pod in %s: %s", namespace, str(e))
        return False

def check_task_07(email, submission):
    namespace = submission.get('namespace', 'default')
    try:
        secret = v1.read_namespaced_secret(name="db-secret", namespace=namespace)
        if secret.data.get("password") != "c3VwZXJzZWNyZXQ=":  # base64 of "supersecret"
            logging.error("Secret db-secret in %s has incorrect data", namespace)
            return False
        logging.info("Secret db-secret in %s is correctly configured", namespace)
        return True
    except client.exceptions.ApiException as e:
        logging.error("Failed to check db-secret in %s: %s", namespace, str(e))
        return False

def check_task_08(email, submission):
    namespace = submission.get('namespace', 'default')
    try:
        pod = v1.read_namespaced_pod(name="secret-pod", namespace=namespace)
        if pod.spec.containers[0].image != "nginx":
            logging.error("Pod secret-pod in %s has incorrect image", namespace)
            return False
        volume = next((v for v in pod.spec.volumes if v.secret and v.secret.secret_name == "db-secret"), None)
        if not volume:
            logging.error("Pod secret-pod in %s does not mount db-secret as a volume", namespace)
            return False
        mount = next((m for m in pod.spec.containers[0].volume_mounts if m.mount_path == "/etc/secret"), None)
        if not mount:
            logging.error("Pod secret-pod in %s does not mount volume at /etc/secret", namespace)
            return False
        logging.info("Pod secret-pod in %s is correctly configured", namespace)
        return True
    except client.exceptions.ApiException as e:
        logging.error("Failed to check secret-pod in %s: %s", namespace, str(e))
        return False

def check_task_09(email, submission):
    namespace = submission.get('namespace', 'jobs')
    try:
        job = batch_v1.read_namespaced_job(name="batch-job", namespace=namespace)
        if job.spec.template.spec.containers[0].image != "perl":
            logging.error("Job batch-job in %s has incorrect image", namespace)
            return False
        if job.spec.template.spec.containers[0].command != ["perl", "-Mbignum=bpi", "-wle", "print bpi(2000)"]:
            logging.error("Job batch-job in %s has incorrect command", namespace)
            return False
        if job.spec.template.spec.restart_policy != "Never":
            logging.error("Job batch-job in %s has incorrect restart policy", namespace)
            return False
        logging.info("Job batch-job in %s is correctly configured", namespace)
        return True
    except client.exceptions.ApiException as e:
        logging.error("Failed to check batch-job in %s: %s", namespace, str(e))
        return False

def check_task_10(email, submission):
    namespace = submission.get('namespace', 'jobs')
    try:
        cronjob = batch_v1.read_namespaced_cron_job(name="daily-task", namespace=namespace)
        if cronjob.spec.schedule != "*/5 * * * *":
            logging.error("CronJob daily-task in %s has incorrect schedule", namespace)
            return False
        if cronjob.spec.job_template.spec.template.spec.containers[0].image != "busybox":
            logging.error("CronJob daily-task in %s has incorrect image", namespace)
            return False
        if cronjob.spec.job_template.spec.template.spec.containers[0].command != ["echo", "Hello CKAD"]:
            logging.error("CronJob daily-task in %s has incorrect command", namespace)
            return False
        if cronjob.spec.job_template.spec.template.spec.restart_policy != "OnFailure":
            logging.error("CronJob daily-task in %s has incorrect restart policy", namespace)
            return False
        logging.info("CronJob daily-task in %s is correctly configured", namespace)
        return True
    except client.exceptions.ApiException as e:
        logging.error("Failed to check daily-task in %s: %s", namespace, str(e))
        return False

def check_task_11(email, submission):
    namespace = submission.get('namespace', 'default')
    try:
        green_deployment = apps_v1.read_namespaced_deployment(name="webapp-green", namespace=namespace)
        if green_deployment.spec.replicas != 3:
            logging.error("Deployment webapp-green in %s has incorrect replicas", namespace)
            return False
        if green_deployment.spec.template.spec.containers[0].image != "nginx:1.16":
            logging.error("Deployment webapp-green in %s has incorrect image", namespace)
            return False
        if not all(label in green_deployment.spec.template.metadata.labels.items() for label in [("app", "webapp"), ("version", "green")]):
            logging.error("Deployment webapp-green in %s has incorrect labels", namespace)
            return False
        service = v1.read_namespaced_service(name="webapp-service", namespace=namespace)
        if not all(label in service.spec.selector.items() for label in [("app", "webapp"), ("version", "green")]):
            logging.error("Service webapp-service in %s does not select webapp-green", namespace)
            return False
        logging.info("Blue-green deployment in %s is correctly configured", namespace)
        return True
    except client.exceptions.ApiException as e:
        logging.error("Failed to check blue-green deployment in %s: %s", namespace, str(e))
        return False

def check_task_12(email, submission):
    namespace = submission.get('namespace', 'limited')
    try:
        quota = v1.read_namespaced_resource_quota(name="compute-quota", namespace=namespace)
        if quota.spec.hard.get("pods") != "10" or quota.spec.hard.get("requests.cpu") != "2":
            logging.error("ResourceQuota compute-quota in %s is misconfigured", namespace)
            return False
        logging.info("ResourceQuota compute-quota in %s is correctly configured", namespace)
        return True
    except client.exceptions.ApiException as e:
        logging.error("Failed to check compute-quota in %s: %s", namespace, str(e))
        return False

def check_task_13(email, submission):
    namespace = submission.get('namespace', 'limited')
    try:
        limitrange = v1.read_namespaced_limit_range(name="default-limits", namespace=namespace)
        container_limit = next((limit for limit in limitrange.spec.limits if limit.type == "Container"), None)
        if not container_limit or container_limit.default.get("cpu") != "500m" or container_limit.default.get("memory") != "512Mi":
            logging.error("LimitRange default-limits in %s is misconfigured", namespace)
            return False
        logging.info("LimitRange default-limits in %s is correctly configured", namespace)
        return True
    except client.exceptions.ApiException as e:
        logging.error("Failed to check default-limits in %s: %s", namespace, str(e))
        return False

def check_task_14(email, submission):
    namespace = submission.get('namespace', 'default')
    try:
        pod = v1.read_namespaced_pod(name="liveness-pod", namespace=namespace)
        if pod.spec.containers[0].image != "nginx:1.14.2":
            logging.error("Pod liveness-pod in %s has incorrect image", namespace)
            return False
        probe = pod.spec.containers[0].liveness_probe
        if not probe or not probe.http_get or probe.http_get.path != "/healthz" or probe.http_get.port != 80:
            logging.error("Pod liveness-pod in %s has incorrect liveness probe path or port", namespace)
            return False
        if probe.period_seconds != 10 or probe.initial_delay_seconds != 5 or probe.failure_threshold != 3:
            logging.error("Pod liveness-pod in %s has incorrect liveness probe timing", namespace)
            return False
        logging.info("Pod liveness-pod in %s is correctly configured", namespace)
        return True
    except client.exceptions.ApiException as e:
        logging.error("Failed to check liveness-pod in %s: %s", namespace, str(e))
        return False

def check_task_15(email, submission):
    namespace = submission.get('namespace', 'default')
    try:
        deployment = apps_v1.read_namespaced_deployment(name="webapp", namespace=namespace)
        if deployment.spec.replicas != 3:
            logging.error("Deployment webapp in %s has incorrect replicas", namespace)
            return False
        probe = deployment.spec.template.spec.containers[0].readiness_probe
        if not probe or not probe.http_get or probe.http_get.path != "/" or probe.http_get.port != 80:
            logging.error("Deployment webapp in %s has incorrect readiness probe path or port", namespace)
            return False
        if probe.period_seconds != 5 or probe.initial_delay_seconds != 3 or probe.success_threshold != 2:
            logging.error("Deployment webapp in %s has incorrect readiness probe timing", namespace)
            return False
        logging.info("Deployment webapp in %s is correctly configured with readiness probe", namespace)
        return True
    except client.exceptions.ApiException as e:
        logging.error("Failed to check webapp readiness probe in %s: %s", namespace, str(e))
        return False

# New function to calculate final weighted grade
def calculate_final_grade(email):
    if email not in user_grades:
        logging.error("No grades found for %s", email)
        return 0.0
    grades = user_grades[email]
    final_grade = 0.0
    for task_id, grade in grades.items():
        if task_id in task_weights:
            # Scale grade (100 or 0) by weight (e.g., 0.05 for 5%)
            weighted_grade = (grade / 100.0) * (task_weights[task_id] * 100.0)
            final_grade += weighted_grade
    logging.info("Calculated final grade for %s: %.2f", email, final_grade)
    return round(final_grade, 2)

# New function to handle finalize task
def check_task_finalize(email, submission):
    final_grade = calculate_final_grade(email)
    return final_grade

# Mapping task_ids to verification functions
task_functions = {
    "task_01": check_task_01,  # Create a Pod with a Specific Image
    "task_02": check_task_02,  # Create a Multi-Container Pod
    "task_03": check_task_03,  # Create a Deployment
    "task_04": check_task_04,  # Expose a Deployment with a Service
    "task_05": check_task_05,  # Create a ConfigMap
    "task_06": check_task_06,  # Use ConfigMap in a Pod
    "task_07": check_task_07,  # Create a Secret
    "task_08": check_task_08,  # Mount a Secret as a Volume
    "task_09": check_task_09,  # Create a Job
    "task_10": check_task_10,  # Create a CronJob
    "task_11": check_task_11,  # Implement a Blue-Green Deployment
    "task_12": check_task_12,  # Create a ResourceQuota
    "task_13": check_task_13,  # Create a LimitRange
    "task_14": check_task_14,  # Configure a Liveness Probe
    "task_15": check_task_15,  # Configure a Readiness Probe
    "task_finalize": check_task_finalize,  # Calculate final grade
}

def process_single_task(task_id, email, submission):
    grade = 0
    if task_id in task_functions:
        task_func = task_functions[task_id]
        if task_id == "task_finalize":
            grade = task_func(email, submission)  # Final grade is a float
        else:
            if task_func(email, submission):
                grade = 100
                logging.info("Task %s for %s passed, assigned grade: %d", task_id, email, grade)
            else:
                grade = 0
                logging.info("Task %s for %s failed, assigned grade: %d", task_id, email, grade)
            # Store grade for user
            if email not in user_grades:
                user_grades[email] = {}
            user_grades[email][task_id] = grade
    else:
        logging.error("Unknown task ID: %s", task_id)
    return grade

def process_message(message):
    logging.info("Processing message: %s", message)
    
    receipt_handle = message.get('ReceiptHandle')
    if not receipt_handle:
        logging.error("Missing receipt handle in message: %s", message)
        return

    try:
        body = json.loads(message['Body'])
    except json.JSONDecodeError as e:
        logging.error("Failed to parse message body: %s (MessageId: %s)", str(e), message.get('MessageId', 'unknown'))
        # Delete the message to prevent looping
        try:
            sqs.delete_message(
                QueueUrl=input_queue_url,
                ReceiptHandle=receipt_handle
            )
            logging.info("Deleted invalid JSON message from input queue: %s", message.get('MessageId', 'unknown'))
        except Exception as e:
            logging.error("Failed to delete invalid JSON message from input queue: %s", str(e))
        return

    # Extract task_submission_id for traceability
    task_submission_id = body.get('task_submission_id', 'unknown')
    email = body.get('email')
    task_id = body.get('task_id')
    submission_str = body.get('submission')

    try:
        if not all([email, task_id, submission_str]):
            raise ValueError(f"Invalid message format: missing email, task_id, or submission (task_submission_id: {task_submission_id})")

        # Parse the submission string into a dictionary
        try:
            # Handle case where submission is already a dict (for compatibility)
            if isinstance(submission_str, dict):
                submission = submission_str
            else:
                submission = json.loads(submission_str)
        except json.JSONDecodeError as e:
            logging.error("Failed to parse submission JSON: %s (task_submission_id: %s)", str(e), task_submission_id)
            # Delete the message to prevent looping
            try:
                sqs.delete_message(
                    QueueUrl=input_queue_url,
                    ReceiptHandle=receipt_handle
                )
                logging.info("Deleted message with invalid submission JSON from input queue: %s", message.get('MessageId', 'unknown'))
            except Exception as e:
                logging.error("Failed to delete message with invalid submission JSON: %s", str(e))
            return

        grade = process_single_task(task_id, email, submission)

        response_message = {
            'task_submission_id': task_submission_id,  # Include task_submission_id in response
            'email': email,
            'task_id': task_id,
            'submission': submission_str,  # Keep original submission for consistency
            'grade': grade
        }
        
        try:
            sqs.send_message(QueueUrl=output_queue_url, MessageBody=json.dumps(response_message))
            logging.info("Sent response to output queue with grade: %d (task_submission_id: %s)", grade, task_submission_id)
        except Exception as e:
            logging.error("Failed to send response to output queue: %s (task_submission_id: %s)", str(e), task_submission_id)

    except ValueError as e:
        logging.error("%s (task_submission_id: %s)", str(e), task_submission_id)
    except Exception as e:
        logging.error("Error processing message: %s (task_submission_id: %s)", str(e), task_submission_id)
    finally:
        try:
            sqs.delete_message(
                QueueUrl=input_queue_url,
                ReceiptHandle=receipt_handle
            )
            logging.info("Deleted message from input queue: %s (task_submission_id: %s)", message.get('MessageId', 'unknown'), task_submission_id)
        except Exception as e:
            logging.error("Failed to delete message from input queue: %s (task_submission_id: %s)", str(e), task_submission)

def poll_sqs_queue():
    while True:
        try:
            response = sqs.receive_message(
                QueueUrl=input_queue_url,
                MaxNumberOfMessages=1,
                WaitTimeSeconds=20
            )
            if 'Messages' in response:
                for message in response['Messages']:
                    process_message(message)
            else:
                logging.debug("No messages received from input queue")
        except Exception as e:
            logging.error("Error polling SQS queue: %s", str(e))
            continue

if __name__ == '__main__':
    logging.info("Starting SQS queue polling")
    poll_sqs_queue()