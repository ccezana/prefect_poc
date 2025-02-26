from demo_flow import schedule_detector_test_flow, calc_engine_flow
from prefect.deployments import run_deployment
# calc_engine_flow.serve(name="scheduled_calc_flow", cron='*/30 * * * * *')

if __name__ == "__main__":
    # schedule_detector_test_flow()
    schedule_detector_test_flow.from_source(
        source="C:\\projects\\prefect_demo",
        entrypoint="deploy.py:schedule_detector_test_flow"
    ).serve(
        name=f"Detector test deployment",
        # cron='*/30 * * * * *',
    )
