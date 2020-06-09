FROM  w4il0rd/redcap_omop_etl_demo:gamma
MAINTAINER r1v3rj1s <djieastgo@yahoo.com>
RUN conda update -y conda && conda update -y --all

ADD fieldmap_etl_v4.py /
CMD ["python", "./fieldmap_etl_v4.py", "/mnt/fieldmap_v4.csv"]
