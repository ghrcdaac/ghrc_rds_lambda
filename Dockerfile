FROM public.ecr.aws/lambda/python:3.12

RUN dnf -y install zip \
    && dnf clean all

ENV package_dir=/rds_lambda/
RUN mkdir ${package_dir}
WORKDIR ${package_dir}
COPY requirements.txt .
RUN pip install --target . -r requirements.txt

ENV package_name=ghrc_rds_lambda.zip
RUN zip -rm ${package_name} .

ENV task_dir=${package_dir}/task
RUN mkdir ${task_dir}
COPY task/ ${task_dir}
RUN zip -rum ${package_name} .
RUN mv ${package_name} /
