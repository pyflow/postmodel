__all__ = (
    "BaseORMException",
    "FieldError",
    "ConfigurationError",
    "TransactionManagementError",
    "OperationalError",
    "IntegrityError",
    "NoValuesFetched",
    "MultipleObjectsReturned",
    "DoesNotExist",
)


class BaseORMException(Exception):
    """
    Base ORM Exception.
    """


class FieldError(BaseORMException):
    """
    The FieldError exception is raised when there is a problem with a model field.
    """


class FieldValueError(BaseORMException):
    """
    The FieldError exception is raised when there is a problem with a model field.
    """

class ParamsError(BaseORMException):
    """
    The ParamsError is raised when function can not be run with given parameters
    """

class PrimaryKeyIntegrityError(BaseORMException):
    """
    The PrimaryKeyIntegrityError is raised when only parts of multi primay keys provided
    """

class PrimaryKeyChangedError(BaseORMException):
    """
    The PrimaryKeyChangedError is raised when try to modify the value of primary key.
    """


class ConfigurationError(BaseORMException):
    """
    The ConfigurationError exception is raised when the configuration of the ORM is invalid.
    """


class TransactionManagementError(BaseORMException):
    """
    The TransactionManagementError is raised when any transaction error occurs.
    """


class OperationalError(BaseORMException):
    """
    The OperationalError exception is raised when an operational error occurs.
    """


class IntegrityError(OperationalError):
    """
    The IntegrityError exception is raised when there is an integrity error.
    """

class StaleObjectError(OperationalError):
    """
    """

class NoValuesFetched(OperationalError):
    """
    The NoValuesFetched exception is raised when the related model was never fetched.
    """


class MultipleObjectsReturned(OperationalError):
    """
    The MultipleObjectsReturned exception is raised when doing a ``.get()`` operation,
    and more than one object is returned.
    """


class DoesNotExist(OperationalError):
    """
    The DoesNotExist exception is raised when expecting data, such as a ``.get()`` operation.
    """


class DBConnectionError(BaseORMException):
    """
    The DBConnectionError is raised when problems with connecting to db occurs
    """
