from flask_wtf import FlaskForm
from wtforms import StringField, TextAreaField, SelectField, BooleanField, FileField
from wtforms.validators import DataRequired, Optional


class TestCaseForm(FlaskForm):
    """Form for creating and editing test cases."""

    tcid = StringField('TCID', validators=[DataRequired()])
    tc_name = StringField('Test Case Name', validators=[DataRequired()])
    table_name = StringField('Table Name', validators=[DataRequired()])
    test_type = StringField('Test Type', validators=[DataRequired()])
    test_yn = BooleanField('Test?')

    src_connection_id = StringField('Source Connection', validators=[Optional()])
    tgt_connection_id = StringField('Target Connection', validators=[Optional()])
    delimiter = StringField('Delimiter', validators=[Optional()])
    filters = StringField('Filters', validators=[Optional()])
    pk_columns = StringField('PK Columns', validators=[DataRequired()])
    date_fields = StringField('Date Fields', validators=[Optional()])
    percentage_fields = StringField('Percentage Fields', validators=[Optional()])
    threshold_percentage = StringField('Threshold Percentage', validators=[Optional()])
    header_columns = StringField('Header Columns', validators=[Optional()])
    skip_rows = StringField('Skip Rows', validators=[Optional()])
    src_sheet_name = StringField('Source Sheet Name', validators=[Optional()])
    tgt_sheet_name = StringField('Target Sheet Name', validators=[Optional()])

    src_input_type = SelectField(
        'Source Input Type', choices=[('query', 'Query'), ('file', 'File')], validators=[DataRequired()]
    )
    tgt_input_type = SelectField(
        'Target Input Type', choices=[('query', 'Query'), ('file', 'File')], validators=[DataRequired()]
    )
    src_query = TextAreaField('Source Query', validators=[Optional()])
    tgt_query = TextAreaField('Target Query', validators=[Optional()])
    src_file = FileField('Source File', validators=[Optional()])
    tgt_file = FileField('Target File', validators=[Optional()])

    class Meta:
        csrf = False

    def validate(self, extra_validators=None):
        if not super().validate(extra_validators=extra_validators):
            return False

        if self.src_input_type.data == 'query' and not self.src_query.data:
            self.src_query.errors.append('Source query is required.')
            return False
        if self.src_input_type.data == 'file' and (
            not self.src_file.data or not getattr(self.src_file.data, 'filename', '')
        ):
            self.src_file.errors.append('Source file is required.')
            return False
        if self.tgt_input_type.data == 'query' and not self.tgt_query.data:
            self.tgt_query.errors.append('Target query is required.')
            return False
        if self.tgt_input_type.data == 'file' and (
            not self.tgt_file.data or not getattr(self.tgt_file.data, 'filename', '')
        ):
            self.tgt_file.errors.append('Target file is required.')
            return False

        return True
