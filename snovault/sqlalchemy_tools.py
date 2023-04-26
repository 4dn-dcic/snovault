import contextlib
import transaction

from snovault import DBSESSION
from snovault.storage import Base
from sqlalchemy import MetaData
from zope.sqlalchemy import mark_changed


# Once debugged, this support probably wants to move to snovault.

class PyramidAppManager:

    def __init__(self, app):
        self.session = app.registry[DBSESSION]
        self._meta = None
        self._ordered_table_names = None

    def _reflect(self):
        if self._meta is None:
            meta = MetaData(bind=self.session.connection())
            meta.reflect()
            self._meta = meta

    @property
    def meta(self):
        self._reflect()
        return self._meta

    @property
    def ordered_table_names(self):
        ordered_names = self._ordered_table_names
        if ordered_names is None:
            self._reflect()
            self._ordered_table_names = ordered_names = reversed(Base.metadata.sorted_tables)
        return ordered_names

    @contextlib.contextmanager
    def connection(self, as_transaction=False):
        """
        Context manager executes a body of code with a connection object to the database.

        :param as_transaction: If the action is expected to be read-only, this can be false.
            If there will be modifications that need to be committed, specify as_transaction=True.

        """
        connection = self.session.connection().connect()
        if not transaction:
            yield connection
        else:
            try:
                yield connection
            except Exception:
                transaction.abort()
            else:
                # commit all changes to DB
                self.session.flush()
                mark_changed(self.session())
                transaction.commit()
