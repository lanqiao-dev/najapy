import copy
from datetime import datetime
from io import BytesIO
import xlrd
from xlwt import Workbook, XFStyle, Borders, Pattern

from najapy.common.async_base import Utils


class ExcelRD():
    """excel读取类"""

    def __init__(self, filename=None, file_contents=None, sheet_name=None):
        self.filename = filename
        self.file_contents = file_contents

        if self.filename:
            self.data = xlrd.open_workbook(filename=filename)
        else:
            self.data = xlrd.open_workbook(file_contents=file_contents)

        if sheet_name:
            self.sheet = self.data.sheet_by_name(sheet_name)
        else:
            self.sheet = self.data.sheet_by_index(0)

        self.header_infos = self.sheet.row_values(0)  # 表头信息

        self.row_nums = self.sheet.nrows  # 全部行数
        self.col_nums = self.sheet.ncols  # 全部列数

        self._field_dict = None

    @property
    def field_dict(self):
        """
        field_dict: 表头信息和代码的映射关系字典 {"name": "姓名", "section": "考勤组", "user_id": "UserId"}
        """
        return self._field_dict

    @field_dict.setter
    def field_dict(self, value):
        """
        field_dict:  {"name": "姓名", "age": "年龄"}
        """
        self._field_dict = value

    @property
    def field_name_dict(self):
        """
        field_name_dict: {"姓名": "name", "年龄": "age"}
        """
        if not self._field_dict:
            return None

        res = {}

        for key, value in self._field_dict.items():
            res[value] = key

        return res

    def _get_fields_col_relation_by_field_dict(self):
        """
        根据表头字段，获取字段和所在列的对应关系
        :return: {"name":1, "age": 3}  ==> {字段：所在列}
        """
        row_data = self.sheet.row_values(0)  # 获取表头信息

        new_row_data = []
        for _row_data in row_data:
            if isinstance(_row_data, float):
                _row_data = str(int(_row_data))

            new_row_data.append(_row_data)

        relation = {}
        for field, field_name in self._field_dict.items():
            if not isinstance(field, str):
                continue

            for index, val in enumerate(new_row_data):

                if field_name.strip() == str(val).strip():
                    relation[field] = index

        return relation

    def _get_values_by_fields(self, fields_relation, start=1):
        """
        :param fields_relation: {"name":1, "age": 3}
        :return: [{"name":"李四", "age": "12"}, {"name":"张三", "age": "16"}]
        """

        rows = []
        for index in range(start, self.row_nums):
            row_data = self.sheet.row_values(index)
            row = {}
            fields = fields_relation.keys()

            for field in fields:
                val_index = fields_relation[field]
                row[field] = str(row_data[val_index])

            rows.append(row)

        return rows

    def get_excel_data(self, start=1):
        """
        根据excel表头和脚本的映射关系获取excel数据
        """
        result = None

        relation = self._get_fields_col_relation_by_field_dict()

        if len(relation) != len(self._field_dict):
            shorted_field = []

            for field, field_str in self._field_dict.items():
                if field not in relation:
                    shorted_field.append(field_str)

            Utils.log.error(f"excel_parsing_data rows error, relation:{relation},shorted_field:{shorted_field}")
            return result

        try:
            rows = self._get_values_by_fields(relation, start)
            result = rows
        except Exception as err:
            Utils.log.error(f"excel_parsing_data rows error:{err},relation:{relation}")

        return result

    def get_all_datas(self):
        """
        获取表格内的所有数据并获取单元格行号和列号
        """
        datas = []

        for i in range(1, self.row_nums):

            sheet_dict = {}

            for j in range(self.col_nums):
                # 获取该单元格所在列的表头名称
                header_name = self.header_infos[j]
                # 获取单元格数据
                cell_data = self.sheet.cell_value(i, j)

                # key值为field_dict中表头名称对应的值或者所在列号
                data_key = str(self.field_name_dict.get(header_name, j))
                sheet_dict[data_key] = {
                    "row_num": i,
                    "col_num": j,
                    "cell_data": cell_data
                }

            datas.append(sheet_dict)

        return datas


class ExcelWT(Workbook):
    """Excel生成工具
    """

    def __init__(self, name, encoding=r'utf-8', style_compression=0):

        super().__init__(encoding, style_compression)

        self._book_name = name
        self._current_sheet = None

        self._default_style = XFStyle()
        self._default_style.borders.left = Borders.THIN
        self._default_style.borders.right = Borders.THIN
        self._default_style.borders.top = Borders.THIN
        self._default_style.borders.bottom = Borders.THIN
        self._default_style.pattern.pattern = Pattern.SOLID_PATTERN
        self._default_style.pattern.pattern_fore_colour = 0x01

        self._default_title_style = copy.deepcopy(self._default_style)
        self._default_title_style.font.bold = True
        self._default_title_style.pattern.pattern_fore_colour = 0x16

    def create_sheet(self, name, titles=None):

        sheet = self._current_sheet = self.add_sheet(name)
        style = self._default_title_style

        if titles:
            for index, title in enumerate(titles):
                sheet.write(0, index, title, style)
                sheet.col(index).width = 0x1200

    def add_sheet_row(self, *args):

        sheet = self._current_sheet
        style = self._default_style

        nrow = len(sheet.rows)

        for index, value in enumerate(args):
            sheet.write(nrow, index, value, style)

    def get_file(self):

        result = b''

        with BytesIO() as stream:
            self.save(stream)

            result = stream.getvalue()

        return result

    def write_request(self, request):

        filename = f"{self._book_name}.{datetime.today().strftime('%y%m%d.%H%M%S')}.xls"

        request.set_header(r'Content-Type', r'application/vnd.ms-excel')
        request.set_header(r'Content-Disposition', f'attachment;filename={filename}')

        return request.finish(self.get_file())
