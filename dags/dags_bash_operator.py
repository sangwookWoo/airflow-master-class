from airflow import DAG
from airflow.operators.bash import BashOperator
import datetime
import pendulum

with DAG(
    # dag 파일명과 dag_id를 일치시키는 것이 좋습니다.
    dag_id="dags_bash_operator",
    # 분 시 일 월 요일
    schedule="0 0 * * *",
    # dag가 언제부터 시작 할 건지 결정
    # utc는 세계 표준시라고 부르고, 한국보다 9시간 느리다.
    # Asia/Seoul은 한국 시간으로
    start_date=pendulum.datetime(2021, 1, 1, tz="Asia/Seoul"),
    # catchup 파라미터
    # catchup 변수가 True면 시작 시간부터 오늘까지 그 사이에 누락된 구간을 전부 한꺼번에 돌립니다.
    # 일반적으로는 catchup 변수는 False로 두는 것이 좋습니다.
    catchup=False,
    # dagrun_timeout은 이 dag가 60분 이상 지나면, dag를 종료한다는 것입니다.
    # dagrun_timeout=datetime.timedelta(minutes=60),
    # tag들은 DAG 들을 모아서 보기 좋게 하기 위해서 설정하는 것입니다.
    # tags=["example", "example2"],
) as dag:
    # bash_t1은 태스크명
    bash_t1 = BashOperator(
        # 이 task_id는 graph에 나오는 이름
        # 이 아이디는 변수명과 사실 관계는 없지만,
        # 객체명과 동일하게 줘야 찾기 편합니다.
        task_id="bash_t1",
        bash_command="echo whoami",
    )

    bash_t2 = BashOperator(
        task_id="bash_t2",
        bash_command="echo $HOSTNAME",
    )

    # 태스크들의 실행 순서
    bash_t1 >> bash_t2
