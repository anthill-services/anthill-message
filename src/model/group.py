
from tornado.gen import coroutine, Return
from common.model import Model
from common.database import DatabaseError, DuplicateError
from history import MessageError, MessagesHistoryModel

from . import CLASS_GROUP


class GroupAdapter(object):
    def __init__(self, data):
        self.group_id = data.get("group_id")
        self.group_class = str(data.get("group_class"))
        self.key = str(data.get("group_key"))


class GroupParticipationAdapter(object):
    def __init__(self, data):
        self.participation_id = data.get("participation_id")
        self.group_id = data.get("group_id")
        self.account = data.get("participation_account")
        self.role = data.get("participation_role")


class GroupAndParticipationAdapter(GroupAdapter, GroupParticipationAdapter):
    def __init__(self, data):
        GroupAdapter.__init__(self, data)
        GroupParticipationAdapter.__init__(self, data)


class GroupsModel(Model):
    def __init__(self, db, history):
        self.db = db
        self.history = history

    def get_setup_tables(self):
        return ["groups", "group_participants"]

    def get_setup_db(self):
        return self.db

    @coroutine
    def add_group(self, gamespace, group_class, key):

        try:
            group_id = yield self.db.insert(
                """
                    INSERT INTO `groups`
                    (`gamespace_id`, `group_class`, `group_key`)
                    VALUES (%s, %s, %s);
                """, gamespace, group_class, key)
        except DuplicateError:
            raise GroupExistsError()
        except DatabaseError as e:
            raise GroupError("Failed to add a group: " + e.args[1])
        else:
            raise Return(group_id)

    @coroutine
    def get_group(self, gamespace, group_id):
        try:
            message = yield self.db.get(
                """
                    SELECT *
                    FROM `groups`
                    WHERE `group_id`=%s AND `gamespace_id`=%s;
                """, group_id, gamespace)
        except DatabaseError as e:
            raise GroupError("Failed to get a group: " + e.args[1])

        if not message:
            raise GroupNotFound()

        raise Return(GroupAdapter(message))

    @coroutine
    def find_group(self, gamespace, group_class, key):
        try:
            message = yield self.db.get(
                """
                    SELECT *
                    FROM `groups`
                    WHERE `gamespace_id`=%s AND `group_class`=%s AND `group_key`=%s;
                """, gamespace, group_class, key)
        except DatabaseError as e:
            raise GroupError("Failed to find a group: " + e.args[1])

        if not message:
            raise GroupNotFound()

        raise Return(GroupAdapter(message))

    @coroutine
    def list_groups(self, gamespace, group_class):
        try:
            groups = yield self.db.query(
                """
                    SELECT *
                    FROM `groups`
                    WHERE `group_class`=%s AND `gamespace_id`=%s;
                """, group_class, gamespace)
        except DatabaseError as e:
            raise GroupError("Failed to list groups: " + e.args[1])

        raise Return(map(GroupAdapter, groups))

    @coroutine
    def delete_group(self, gamespace, group_id):
        try:
            yield self.history.delete_messages(gamespace, CLASS_GROUP, group_id)
        except MessageError as e:
            raise GroupError("Failed to delete group's messages: " + e.message)

        try:
            yield self.db.execute(
                """
                    DELETE FROM `groups`
                    WHERE `group_id`=%s AND `gamespace_id`=%s;
                """, group_id, gamespace)
        except DatabaseError as e:
            raise GroupError("Failed to delete a group: " + e.args[1])

    @coroutine
    def update_group(self, gamespace, group_id, group_class, key):
        try:
            yield self.db.execute(
                """
                    UPDATE `groups`
                    SET `group_class`=%s, `group_key`=%s
                    WHERE `gamespace_id`=%s AND `group_id`=%s;
                """, group_class, key, gamespace, group_id)
        except DatabaseError as e:
            raise GroupError("Failed to update a group: " + e.args[1])

    @coroutine
    def join_group(self, gamespace, group_id, account, role):
        try:
            participation_id = yield self.db.execute(
                """
                    INSERT INTO `group_participants`
                    (gamespace_id, `group_id`, participation_account, participation_role)
                    VALUES (%s, %s, %s, %s);
                """, gamespace, group_id, account, role)
        except DuplicateError:
            raise UserAlreadyJoined()
        except DatabaseError as e:
            raise GroupError("Failed to join a group: " + e.args[1])

        raise Return(participation_id)

    @coroutine
    def get_group_participation(self, gamespace, participation_id):
        try:
            participant = yield self.db.get(
                """
                    SELECT *
                    FROM `group_participants`
                    WHERE `participation_id`=%s AND `gamespace_id`=%s;
                """, participation_id, gamespace)
        except DatabaseError as e:
            raise GroupError("Failed to get group participant: " + e.args[1])

        if not participant:
            raise GroupParticipantNotFound()

        raise Return(GroupParticipationAdapter(participant))

    @coroutine
    def updated_group_participation(self, gamespace, participation_id, role):
        try:
            yield self.db.execute(
                """
                    UPDATE `group_participants`
                    SET `participation_role`=%s
                    WHERE `gamespace_id`=%s AND `participation_id`=%s;
                """, role, gamespace, participation_id)
        except DatabaseError as e:
            raise GroupError("Failed to update a group participation: " + e.args[1])

    @coroutine
    def leave_group(self, gamespace, group_id, account):
        try:
            yield self.db.execute(
                """
                    DELETE FROM `group_participants`
                    WHERE `gamespace_id`=%s AND `group_id`=%s AND `participation_account`=%s;
                """, gamespace, group_id, account)
        except DatabaseError as e:
            raise GroupError("Failed to leave a group: " + e.args[1])

    @coroutine
    def find_group_participant(self, gamespace, group_id, account):
        try:
            participant = yield self.db.get(
                """
                    SELECT *
                    FROM `group_participants`
                    WHERE `group_id`=%s AND `participation_account`=%s AND `gamespace_id`=%s;
                """, group_id, account, gamespace)
        except DatabaseError as e:
            raise GroupError("Failed to get group participant: " + e.args[1])

        if not participant:
            raise GroupParticipantNotFound()

        raise Return(GroupParticipationAdapter(participant))

    @coroutine
    def list_group_participants(self, gamespace, group_id):
        try:
            participants = yield self.db.query(
                """
                    SELECT *
                    FROM `group_participants`
                    WHERE `group_id`=%s AND `gamespace_id`=%s;
                """, group_id, gamespace)
        except DatabaseError as e:
            raise GroupError("Failed to list group participants: " + e.args[1])

        raise Return(map(GroupParticipationAdapter, participants))

    @coroutine
    def list_groups_account_participates(self, gamespace, account_id):
        try:
            groups = yield self.db.query(
                """
                    SELECT g.*, p.*
                    FROM `group_participants` AS p
                        INNER JOIN `groups` AS g
                        ON p.`group_id`=`g`.`group_id`
                    WHERE p.`participation_account`=%s AND p.`gamespace_id`=%s;
                """, account_id, gamespace)
        except DatabaseError as e:
            raise GroupError("Failed to list group account participate: " + e.args[1])

        raise Return(map(GroupAndParticipationAdapter, groups))

    @coroutine
    def list_group_ids_account_participates(self, gamespace, account_id):
        try:
            participants = yield self.db.query(
                """
                    SELECT `group_id`
                    FROM `group_participants`
                    WHERE `participation_account`=%s AND `gamespace_id`=%s;
                """, account_id, gamespace)
        except DatabaseError as e:
            raise GroupError("Failed to list group account participate: " + e.args[1])

        raise Return([participant["group_id"] for participant in participants])


class GroupNotFound(Exception):
    pass


class GroupParticipantNotFound(Exception):
    pass


class GroupExistsError(Exception):
    pass


class UserAlreadyJoined(Exception):
    pass


class GroupError(Exception):
    def __init__(self, message):
        self.message = message
