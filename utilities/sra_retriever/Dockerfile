FROM python:latest

WORKDIR /sra_retriever

COPY requirements.txt ./

RUN pip3 install --no-cache-dir -r requirements.txt
RUN wget https://ftp-trace.ncbi.nlm.nih.gov/sra/sdk/2.10.0/setup-apt.sh
RUN sh setup-apt.sh
RUN apt-get install zlib1g-dev libxml2-dev
RUN cpan install XML::LibXML
RUN cpan install URI

COPY sra_retriever.py /sra_retriever/sra_retriever.py

ENV PATH="/usr/local/ncbi/sra-tools/bin:${PATH}"

CMD python3 sra_retriever.py $PROJECT $SUBSRIPTION $DEST_GCS_DIR_URI $SUCCESS_TOPIC $FAILED_TOPIC
