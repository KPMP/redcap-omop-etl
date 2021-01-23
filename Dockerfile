FROM  w4il0rd/redcap_omop_etl_demo:gamma
MAINTAINER r1v3rj1s <djieastgo@yahoo.com>
RUN conda update -y conda && conda update -y --all
ADD test_auth /
ADD test_input_form.csv /
ADD ../fieldmap_etl_v5.py /
CMD ["python", "./fieldmap_etl_v5.py", "test_input_form.csv", "test_auth", "/mnt/fieldmap_v5.csv"]
