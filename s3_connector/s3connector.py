#!/usr/bin/python
# -*- coding: utf-8 -*-
from __future__ import print_function

__author__ = "bibow"

import boto3, csv, xmltodict, json, base64, pydocparser
import pandas as pd
from io import StringIO, BytesIO
from dicttoxml import dicttoxml
from datetime import datetime
from xml.dom.minidom import parseString
from time import sleep
from tenacity import retry, wait_exponential, stop_after_attempt
from silvaengine_utility import Utility


class S3Connector(object):
    def __init__(self, logger, **setting):
        self.logger = logger
        self.setting = setting
        self.s3 = self.connect()
        if setting.get("docparser_api_key"):
            self.docparser = pydocparser.Parser()
            self.docparser.login(setting.get("docparser_api_key"))

    def connect(self):
        if (
            self.setting.get("region_name")
            and self.setting.get("aws_access_key_id")
            and self.setting.get("aws_secret_access_key")
        ):
            return boto3.client(
                "s3",
                region_name=self.setting.get("region_name"),
                aws_access_key_id=self.setting.get("aws_access_key_id"),
                aws_secret_access_key=self.setting.get("aws_secret_access_key"),
            )
        else:
            return boto3.client("s3")

    @property
    def s3(self):
        return self._s3

    @s3.setter
    def s3(self, s3):
        self._s3 = s3

    @property
    def docparser(self):
        return self._docparser

    @docparser.setter
    def docparser(self, docparser):
        self._docparser = docparser

    def remove_empty_value(self, row):
        new_row = {}
        for key, value in row.items():
            key = key.strip()
            value = value.strip()
            if key != "" and value != "":
                new_row[key] = value
        return new_row

    def archive_object(self, bucket, key):
        self.s3.copy_object(
            CopySource={"Bucket": bucket, "Key": key},
            Bucket=bucket,
            Key=f"archive/{datetime.utcnow().strftime('%Y-%m-%d')}/{key}",
        )

        self.s3.delete_object(Bucket=bucket, Key=key)

    def get_rows(self, bucket, key, new_line="\r\n"):
        rows = []
        obj = self.s3.get_object(Bucket=bucket, Key=key)
        content = obj["Body"].read()
        if key.find(".xlsx") != -1:
            df = pd.read_excel(BytesIO(content))
            for row in df.to_dict("records"):
                row.update({"LastModified": obj["LastModified"]})
                rows.append(row)
        else:
            content = content.decode(self.setting.get("file_encoding", "utf-8"))
            lines = content.split(new_line)
            for row in csv.DictReader(lines):
                row = self.remove_empty_value(row)
                if row:
                    row.update({"LastModified": obj["LastModified"]})
                    rows.append(row)

        # Copy to archive bucket.
        self.archive_object(bucket, key)

        # If there are 200 rows only, return the data.
        if len(rows) <= 200:
            return rows

        # Split file with 200 rows each.
        suffix = 0
        row_max_amount_keys = max(rows, key=lambda row: len(row.keys()))
        keys = row_max_amount_keys.keys()
        for i in range(0, len(rows), 200):
            if key.find(".xlsx") != -1:
                df = pd.DataFrame(rows[i : i + 200])
                date_columns = df.select_dtypes(include=['datetime64[ns, UTC]']).columns
                for date_column in date_columns:
                    df[date_column] = df[date_column].dt.tz_localize(None)
                buffer = BytesIO()
                with pd.ExcelWriter(buffer) as writer:
                    df.to_excel(excel_writer=writer, index=False)
                self.s3.put_object(
                    Bucket=bucket,
                    Key=key.replace(".xlsx", f"-{suffix}.xlsx"),
                    Body=buffer.getvalue(),
                )
            else:
                output = StringIO()
                writer = csv.DictWriter(output, keys, delimiter=",")
                writer.writeheader()
                writer.writerows(rows[i : i + 200])
                self.s3.put_object(
                    Bucket=bucket,
                    Key=key.replace(".csv", f"-{suffix}.csv"),
                    Body=output.getvalue(),
                )
            suffix += 1
            sleep(1)
        return []

    @retry(
        reraise=True,
        wait=wait_exponential(multiplier=1, max=60),
        stop=stop_after_attempt(5),
    )
    def get_one_result(self, parser_name, document_id):
        data = self.docparser.get_one_result(parser_name, document_id)
        if not isinstance(data, list):
            raise Exception(data)
        return data

    def get_data_by_docparser(self, bucket, key, parser_name):
        obj = self.s3.get_object(Bucket=bucket, Key=key)
        encoded_string = base64.b64encode(obj["Body"].read())
        document_id = self.docparser.upload_file_by_base64(
            encoded_string, key.split("/")[-1], parser_name
        )
        # document_id = "d32e57c52d7da004947a2326ee51bd70"
        sleep(10)
        data = self.get_one_result(parser_name, document_id)

        ## Copy to archive bucket.
        self.archive_object(bucket, key)

        return data

    def dict_2_csv(self, data):
        rows = []
        _lists = list(filter(lambda v: isinstance(v, list), data.values()))
        while len(_lists):
            _list = _lists.pop()
            if len(rows) == 0:
                rows = [
                    dict(
                        {
                            k: v
                            for k, v in data.items()
                            if not isinstance(v, (dict, list))
                        },
                        **{k: v for k, v in item.items()},
                    )
                    for item in _list
                ]

                ## load attributes from dict.
                for value in list(filter(lambda v: isinstance(v, dict), data.values())):
                    rows = [
                        dict(row, **{k: v for k, v in value.items()}) for row in rows
                    ]
            else:
                _rows = []
                for row in rows:
                    _rows.extend(
                        [dict(row, **{k: v for k, v in item.items()}) for item in _list]
                    )
                rows = _rows
        return rows

    def put_object(self, key, data):
        bucket = self.setting.get("export_bucket")
        export_type = self.setting.get("export_type")
        output = StringIO()
        if export_type == "xml":
            obj = dicttoxml(data, attr_type=False)
            obj = parseString(obj)
            output.write(obj.toprettyxml())
        elif export_type == "json":
            output.write(Utility.json_dumps(data))
        elif export_type == "csv":
            rows = self.dict_2_csv(data)
            writer = csv.DictWriter(output, rows[0].keys(), delimiter=",")
            writer.writeheader()
            writer.writerows(rows)
        self.s3.put_object(
            Bucket=bucket, Key=f"{key}.{export_type}", Body=output.getvalue()
        )

    def get_matching_s3_keys(self, bucket, prefix="", suffix=""):
        """
        Generate the keys in an S3 bucket.

        :param bucket: Name of the S3 bucket.
        :param prefix: Only fetch keys that start with this prefix (optional).
        :param suffix: Only fetch keys that end with this suffix (optional).
        """
        kwargs = {"Bucket": bucket}

        # If the prefix is a single string (not a tuple of strings), we can
        # do the filtering directly in the S3 API.
        if isinstance(prefix, str):
            kwargs["Prefix"] = prefix

        while True:
            # The S3 API response is a large blob of metadata.
            # 'Contents' contains information about the listed objects.
            resp = self.s3.list_objects_v2(**kwargs)
            for obj in resp["Contents"]:
                key = obj["Key"]
                if key.startswith(prefix) and key.endswith(suffix):
                    yield obj

            # The S3 API is paginated, returning up to 1000 keys at a time.
            # Pass the continuation token into the next response, until we
            # reach the final page (when this field is missing).
            try:
                kwargs["ContinuationToken"] = resp["NextContinuationToken"]
            except KeyError:
                break

    def get_total_objects(self, record_type, cut_date):
        bucket = self.setting.get("import_bucket")
        import_type = self.setting.get("import_type")
        prefix = f"{record_type}/"
        suffix = f".{import_type}"
        objs = self.get_matching_s3_keys(bucket, prefix=prefix, suffix=suffix)

        cut_date = datetime.strptime(cut_date, "%Y-%m-%d %H:%M:%S")
        objs = list(
            filter(lambda t: (t["LastModified"].replace(tzinfo=None) > cut_date), objs)
        )
        return len(objs)

    def get_objects(self, record_type, **kwargs):
        coding = self.setting.get("file_encoding", "utf-8")
        bucket = self.setting.get("import_bucket")
        import_type = self.setting.get("import_type")

        offset = kwargs.get("offset", 0)
        limit = kwargs.get("limit", 100)
        cut_date = kwargs.get("cut_date", None)

        prefix = f"{record_type}/"
        suffix = f".{import_type}"
        _objs = self.get_matching_s3_keys(bucket, prefix=prefix, suffix=suffix)

        if cut_date:
            cut_date = datetime.strptime(cut_date, "%Y-%m-%d %H:%M:%S")
            _objs = list(
                filter(
                    lambda t: (t["LastModified"].replace(tzinfo=None) > cut_date), _objs
                )
            )

        get_last_modified = lambda _obj: int(_obj["LastModified"].strftime("%s"))
        _objs = [_obj for _obj in sorted(_objs, key=get_last_modified)][
            offset : (offset + limit)
        ]

        objs = []
        for _obj in _objs:
            _key = _obj["Key"]
            _lastModified = _obj["LastModified"].strftime("%Y-%m-%d %H:%M:%S")
            _key = _key.replace(prefix, "", 1)
            _key = _key.replace(suffix, "", 1)
            content = self.s3.get_object(Bucket=bucket, Key=_obj["Key"])["Body"].read()
            content = content.decode(coding).encode("utf-8")
            obj = None
            if import_type == "xml":
                if record_type == "inventory":
                    obj = xmltodict.parse(content, force_list={"inventory": True})[
                        "root"
                    ]
                else:
                    obj = xmltodict.parse(content, force_list={"item": True})["root"]
                obj["key"] = _key
                obj["lastModified"] = _lastModified
            elif import_type == "json":
                obj = json.loads(content)
                obj["key"] = _key
                obj["lastModified"] = _lastModified
            objs.append(obj)

            self.s3.copy_object(
                CopySource={"Bucket": bucket, "Key": _obj["Key"]},
                Bucket=bucket,
                Key=f"archive/{datetime.utcnow().strftime('%Y-%m-%d')}/({_obj['Key']})",
            )
            self.s3.delete_object(Bucket=bucket, Key=_obj["Key"])

        return objs
