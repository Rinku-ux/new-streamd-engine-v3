import aiohttp
import asyncio
import json

class RedashClient:
    def __init__(self, endpoint: str, api_key: str):
        self.endpoint = endpoint.rstrip('/')
        self.api_key = api_key
        self.headers = {
            "Authorization": f"Key {self.api_key}",
            "Content-Type": "application/json"
        }

    async def fetch_query(self, query_id: str, parameters: dict = None, timeout_seconds=600) -> list:
        url = f"{self.endpoint}/api/queries/{query_id}/results"
        payload = {"parameters": parameters or {}}
        
        max_retries = 3
        retry_delay = 2.0

        connector = aiohttp.TCPConnector(ssl=False)
        async with aiohttp.ClientSession(headers=self.headers, connector=connector) as session:
            for attempt in range(max_retries):
                try:
                    # 1. Start execution
                    async with session.post(url, json=payload) as resp:
                        if resp.status == 503 or resp.status == 504:
                            # Transient Redash errors
                            raise aiohttp.ClientError(f"Redash Overloaded ({resp.status})")
                        if resp.status != 200:
                            text = await resp.text()
                            raise Exception(f"Redash Error ({resp.status}): {text}")
                        data = await resp.json()

                    # If it returned direct results
                    if "query_result" in data:
                        return data["query_result"]["data"]["rows"]

                    if "job" not in data:
                        raise Exception(f"Unexpected Redash response: {data}")

                    # 2. Wait for Job completion
                    job_id = data["job"]["id"]
                    job_url = f"{self.endpoint}/api/jobs/{job_id}"

                    job_status = 1
                    query_result_id = None
                    poll_interval = 2.0
                    waited = 0.0

                    while job_status in [1, 2]: # 1=PENDING, 2=STARTED
                        if waited >= timeout_seconds:
                            raise TimeoutError(f"Query {query_id} timed out after {timeout_seconds}s.")
                        
                        await asyncio.sleep(poll_interval)
                        waited += poll_interval

                        async with session.get(job_url) as jresp:
                            if jresp.status != 200:
                                # Possible transient network error during polling
                                continue 
                            jdata = await jresp.json()
                            job = jdata.get("job", {})
                            job_status = job.get("status")
                            
                            if job_status == 3: # SUCCESS
                                query_result_id = job.get("query_result_id")
                                break
                            elif job_status == 4: # FAILURE
                                err = job.get("error", "Unknown error")
                                raise Exception(f"Redash Query Execution Failed: {err}")
                            elif job_status == 5: # CANCELED
                                raise Exception("Redash Query Cancelled.")

                    if not query_result_id:
                        raise Exception("Failed to get query_result_id.")

                    # 3. Fetch final result
                    res_url = f"{self.endpoint}/api/query_results/{query_result_id}"
                    async with session.get(res_url) as final_resp:
                        if final_resp.status != 200:
                            raise aiohttp.ClientError(f"Final fetch failed ({final_resp.status})")
                        final_data = await final_resp.json()
                        return final_data["query_result"]["data"]["rows"]
                        
                except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                    if attempt < max_retries - 1:
                        await asyncio.sleep(retry_delay * (2 ** attempt)) # Exponential backoff
                        continue
                    raise e
                except Exception as e:
                    # Non-retryable errors
                    raise e
        return []
