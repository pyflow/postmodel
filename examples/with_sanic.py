# pylint: disable=E0401,E0611
import logging

from sanic import Sanic, response

from postmodel import models
from postmodel.ext.sanic import register_postmodel
from basepy.asynclog import logger

logger.add('stdout')

logging.basicConfig(level=logging.DEBUG)

app = Sanic(__name__)

class Users(models.Model):
    id = models.IntField(pk=True)
    name = models.CharField(50)

    def __str__(self):
        return f"User {self.id}: {self.name}"


@app.route("/")
async def list_all(request):
    users = await Users.all()
    return response.json({"users": [str(user) for user in users]})


@app.route("/user")
async def add_user(request):
    user = await Users.create(name="New User")
    return response.json({"user": str(user)})


register_postmodel(
    app, 'postgres://postgres@localhost:5432/test_db', modules=[__name__], generate_schemas=True
)


if __name__ == "__main__":
    app.run(port=5000)
