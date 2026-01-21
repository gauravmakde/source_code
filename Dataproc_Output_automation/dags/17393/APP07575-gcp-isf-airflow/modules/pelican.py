import requests


class Pelican:

    @staticmethod
    def validate(env, dag_name):
        if len(env) and env.lower() == "prod":
            pelican_validate_api = "https://sharedingress-nsk-beet-prod-us-west-2.nordstromaws.app/app09276/pelican-proxy/validate"
            pelican_result_api = "https://sharedingress-nsk-beet-prod-us-west-2.nordstromaws.app/app09276/pelican-proxy/validationresult"

        else:
            pelican_validate_api = "https://sharedingress-nsk-beet-nonprod-us-west-2.nordstromaws.app/app09276/pelican-proxy-feature-initial-setup/validate"
            pelican_result_api = "https://sharedingress-nsk-beet-nonprod-us-west-2.nordstromaws.app/app09276/pelican-proxy-feature-initial-setup/validationresult"

        body = {'dagName': dag_name}
        print(f"body {body}")
        response = requests.post(url=pelican_validate_api, json=body)
        print(f"response {response}")
        response_body = response.json()
        result_ids = []

        if response.status_code != 200:
            raise Exception(f"Validate POST call failed " + str(response_body))

        for each in response_body:
            print(each)
            result_ids.append(each["requestId"])
        print(result_ids)
        return result_ids

    @staticmethod
    def check_status(request_id, env, dag_name):
        if len(env) and env.lower() == "prod":
            pelican_result_api = "https://sharedingress-nsk-beet-prod-us-west-2.nordstromaws.app/app09276/pelican-proxy/validationresult"
        else:
            pelican_result_api = "https://sharedingress-nsk-beet-nonprod-us-west-2.nordstromaws.app/app09276/pelican-proxy-feature-initial-setup/validationresult"

        param = {'schedulerId': request_id, 'dagName': dag_name}
        result_id_response = requests.get(url=pelican_result_api, params=param)
        status = str(result_id_response.content, encoding='utf-8')
        print(status)
        return status.lower()

    @staticmethod
    def check_job_status(context, dag_name, env):
        request_ids = context['ti'].xcom_pull(task_ids='pelican_validation')
        print(f"request_ids {request_ids} to check job status of given DAG")
        result_id_responses = [Pelican.check_status(request_id, dag_name=dag_name, env=env) for request_id in request_ids]
        print(f"status of each of above request_ids {result_id_responses}")
        if any(status == "missing" for status in result_id_responses):
            raise Exception(f"Scheduler is missing for given DAG: {dag_name}")
        if any(status == "failed" for status in result_id_responses):
            raise Exception(f"Validation failed for given DAG: {dag_name}")
        return all(status in ["success", "completed", "done"] for status in result_id_responses)
