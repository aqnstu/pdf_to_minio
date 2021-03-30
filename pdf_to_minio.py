# coding: utf-8
from config import *
from loguru import logger
from minio import Minio
from pdf2image import convert_from_bytes
from pyminio import Pyminio
from sqlalchemy import Column, DateTime, LargeBinary, VARCHAR, text, create_engine, Table, MetaData
from sqlalchemy.dialects.oracle import NUMBER
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm.session import sessionmaker
import io
import itertools
import timeit

logger.add('pdf_to_minio.log', backtrace=True, diagnose=True, level='DEBUG')

engine_oracle = create_engine(
    f'{DB}+{DRIVER}://{USERNAME}:{PASSWORD}@{HOST}:{PORT}/{DB_NAME}',
    echo=False
)

try:
    engine_oracle.connect()
except:
    logger.exception("Database access problem!")

factory = sessionmaker(
    bind=engine_oracle,
    autocommit=False, 
    autoflush=False
)
session = factory()

minio_obj = Minio(
    MINIO_URL,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False
)
pyminio_client = Pyminio(minio_obj=minio_obj)

Base = declarative_base()
metadata = Base.metadata


class PdfDoc(Base):
    __tablename__ = 'pdf_doc'
    __table_args__ = {'schema': 'decanatuser'}

    pk = Column(NUMBER(asdecimal=False), primary_key=True)
    fk_pdf_doc_type = Column(NUMBER(asdecimal=False),
                             nullable=False, index=True)
    name = Column(VARCHAR(512), nullable=False)
    body = Column(LargeBinary)
    publish_date = Column(DateTime, nullable=False)
    fk_decanat_user = Column(NUMBER(asdecimal=False), index=True)
    hash = Column(VARCHAR(64), index=True, comment='хэш идентификатора')
    fk_mime_type = Column(NUMBER(asdecimal=False), index=True, server_default=text("""\
1
"""), comment='тит документа')
    minio_id = Column(VARCHAR(1024))
    minio_cnt = Column(NUMBER(asdecimal=False), server_default=text("""\
0
"""))


t_prl_ohop_pdf = Table(
    'prl_ohop_pdf', metadata,
    Column('pk', NUMBER(asdecimal=False), nullable=False),
    Column('id_curr', VARCHAR(300)),
    Column('publish_date', DateTime, nullable=False),
    schema='decanatuser'
)


def main():
    try:
        pdfs = session.query(
            PdfDoc
        ).join(
            t_prl_ohop_pdf, PdfDoc.pk == t_prl_ohop_pdf.c.pk
        ).all()
    except:
        logger.exception(f"Something wrong with query to DB!")

    for pdf in pdfs:
        try:
            images = convert_from_bytes(
                pdf.body,
                dpi=200,
                fmt='jpeg',
                jpegopt={'quality': 85, 'optimize': True,
                         'progressive': False},
            )
        except:
            logger.exception(f"PK: {int(pdf.pk)}")
            continue
        pyminio_client.mkdirs(f'/pdf/{int(pdf.pk)}/')
        num = 1
        for image in images:
            in_memory_file = io.BytesIO()
            image.save(in_memory_file, format=image.format)
            pyminio_client.put_data(
                path=f'/pdf/{int(pdf.pk)}/{str(num).zfill(3)}.jpeg',
                data=in_memory_file.getvalue(),
                metadata={'publish_date': str(pdf.publish_date)}
            )
            num += 1
        cnt = len(images)
        session.query(
            PdfDoc
        ).filter(
            PdfDoc.pk == pdf.pk
        ).update(
            {
                PdfDoc.minio_id: f'{int(pdf.pk)}/{str(1).zfill(3)}',
                PdfDoc.minio_cnt: cnt
            }
        )
        session.commit()


if __name__ == '__main__':
    start_time = timeit.default_timer()
    main()
    end_time = timeit.default_timer() - start_time
    print(f'Time: {end_time}')
    logger.debug(f'Time: {end_time}')
