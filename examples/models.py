from tortoise import Model, fields


class Test(Model):
    name = fields.CharField(max_length=255)
