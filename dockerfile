FROM apache/spark:3.5.9-python3

USER root

# 1. Spark 기존 JAR 제거 - 구버전 문제 해결 (libthrift-0.12.0.jar 제거)
RUN rm -f /opt/spark/jars/libthrift-0.12.0.jar
# 1. 드라이버,커넥터 JAR 다운로드 
COPY jars/*.jar /opt/spark/jars/

# 변환 PySpark 스크립트 복사
WORKDIR /opt/spark/work-dir
COPY app.py /opt/spark/work-dir/app.py

# 필요한 Python 패키지 설치
RUN pip install --no-cache-dir apache-iotdb python-dotenv

# 기본 Spark 유저
USER 185 

ENTRYPOINT ["/opt/entrypoint.sh"]