# coding=utf-8

from wikilife_data.dao.crawler.twitter.twitter_reader_dao import \
    TwitterReaderDAO
from wikilife_data.dao.crawler.twitter.twitter_user_dao import TwitterUserDAO
from wikilife_data.dao.developers.developers_dao import DevelopersDAO
from wikilife_data.dao.location.location_dao import LocationDAO
from wikilife_data.dao.logs.log_dao import LogDAO
from wikilife_data.dao.meta.meta_dao import MetaDAO
from wikilife_data.dao.oauth.app_dao import AppDAO
from wikilife_data.dao.oauth.client_dao import ClientDAO
from wikilife_data.dao.oauth.token_dao import TokenDAO
from wikilife_data.dao.processors.final_log_dao import FinalLogDAO
from wikilife_data.dao.processors.final_log_processor_status_dao import \
    FinalLogProcessorStatusDAO
from wikilife_data.dao.processors.generic_dao import GenericDAO
from wikilife_data.dao.processors.processor_status_dao import ProcessorStatusDAO
from wikilife_data.dao.stats.aggregation_dao import AggregationDAO
from wikilife_data.dao.stats.user_option_last_log_dao import \
    UserOptionLastLogDAO
from wikilife_data.dao.user.profile_dao import ProfileDAO
from wikilife_data.dao.user.user_dao import UserDAO
from wikilife_data.dao.user.user_token_dao import UserTokenDAO


class DAOBuilder(object):

    _logger = None
    _db_conn = None

    def __init__(self, logger, db_conn):
        self._logger = logger
        self._db_conn = db_conn


    """ Main DB related"""

    """ live """
    def build_live_meta_dao(self):
        return MetaDAO(self._logger, self._db_conn.get_conn_meta_live())
    
    """
    def build_live_question_dao(self):
        return QuestionDAO(self._logger, self._db_conn.get_conn_main_live())

    def build_live_security_question_dao(self):
        return SecurityQuestionDAO(self._logger, self._db_conn.get_conn_main_live())
    """

    """ edit """

    def build_edit_meta_dao(self):
        return MetaDAO(self._logger, self._db_conn.get_conn_meta_edit())
    
    """
    def build_edit_question_dao(self):
        return QuestionDAO(self._logger, self._db_conn.get_conn_main_edit())
    """

    """
    def build_edit_questions_tag_dao(self):
        return QuestionsTagDAO(self._logger, self._db_conn.get_conn_main_edit())

    def build_edit_security_question_dao(self):
        return SecurityQuestionDAO(self._logger, self._db_conn.get_conn_main_edit())
    """

    """
    def build_location_managers(self):
        db = self._get_main_db()
        countries_mgr = CountriesDAO(self._logger, db)
        regions_mgr = RegionsDAO(self._logger, db)
        cities_mgr = CitiesDAO(self._logger, db)
        return countries_mgr, regions_mgr, cities_mgr
    """

    """ Users DB related """

    def build_user_dao(self):
        return UserDAO(self._logger, self._db_conn.get_conn_users())

    def build_twitter_user_dao(self):
        return TwitterUserDAO(self._logger, self._db_conn.get_conn_users())

    #twitter_users_hash_tmp

    def build_user_token_dao(self):
        return UserTokenDAO(self._logger, self._db_conn.get_conn_users())
    
    """
    def build_recovery_information_dao(self):
        return RecoveryInformationDAO(self._logger, self._db_conn.get_conn_users())

    def build_twitter_configuration_dao(self):
        return TwitterConfigurationDAO(self._logger, self._db_conn.get_conn_users())

    def build_user_services_dao(self):
        return UserServicesDAO(self._logger, self._db_conn.get_conn_users())
    """

    """ Logs DB related """

    def build_log_dao(self):
        return LogDAO(self._logger, self._db_conn.get_conn_logs())
    
    """
    def build_answer_dao(self):
        return AnswerDAO(self._logger, self._db_conn.get_conn_logs())
    """

    """ Admin DB related """

    """
    def build_admin_user_dao(self):
        return AdminUserDAO(self._logger, self._db_conn.get_conn_admin())

    def build_staging_dao(self):
        return StagingDAO(self._logger, self._db_conn.get_conn_admin())

    def build_release_dao(self):
        return ReleaseDAO(self._logger, self._db_conn.get_conn_admin())

    def build_history_dao(self):
        return HistoryDAO(self._logger, self._db_conn.get_conn_admin())
    """

    """ Crawler DB related """

    def build_twitter_reader_dao(self):
        return TwitterReaderDAO(self._logger, self._db_conn.get_conn_crawler())

    """ Processors DB related """

    def build_processor_status_dao(self):
        return ProcessorStatusDAO(self._logger, self._db_conn.get_conn_processors())
    
    """
    def build_cronned_processor_status_dao(self):
        return CronnedProcessorStatusDAO(self._logger, self._db_conn.get_conn_processors())
    """
    def build_generic_dao(self):
        return GenericDAO(self._logger, self._db_conn.get_conn_processors())

    def build_final_log_processor_status_dao(self):
        return FinalLogProcessorStatusDAO(self._logger, self._db_conn.get_conn_processors())

    def build_profile_dao(self):
        return ProfileDAO(self._logger, self._db_conn.get_conn_processors())
    
    """
    def build_timeline_dao(self):
        return TimelineDAO(self._logger, self._db_conn.get_conn_processors())

    def build_stats_dao(self):
        return StatsDAO(self._logger, self._db_conn.get_conn_processors())

    def build_daily_stats_dao(self):
        return DailyStatsDAO(self._logger, self._db_conn.get_conn_processors())

    def build_generic_stats_dao(self):
        return GenericStatsDAO(self._logger, self._db_conn.get_conn_processors())

    def build_reports_dao(self):
        return ReportsDAO(self._logger, self._db_conn.get_conn_processors())

    def build_generic_daily_stats_dao(self):
        return GenericDailyStatsDAO(self._logger, self._db_conn.get_conn_processors())
    """
    def build_final_log_dao(self):
        return FinalLogDAO(self._logger, self._db_conn.get_conn_processors())

    def build_user_option_last_log_dao(self):
        return UserOptionLastLogDAO(self._logger, self._db_conn.get_conn_processors())
    """
    def build_user_log_stats_dao(self):
        return UserLogStatsDAO(self._logger, self._db_conn.get_conn_processors())

    def build_generic_global_log_stats_dao(self):
        return GenericGlobalLogStatsDAO(self._logger, self._db_conn.get_conn_processors())

    def build_profile_report_dao(self):
        return ProfileReportDAO(self._logger, self._db_conn.get_conn_processors())

    def build_node_daily_dao(self):
        return NodeDailyDAO(self._logger, self._db_conn.get_conn_processors())

    def build_exercise_dao(self):
        return ExerciseDAO(self._logger, self._db_conn.get_conn_processors())
    """

    def build_aggregation_dao(self):
        return AggregationDAO(self._logger, self._db_conn.get_conn_processors())

    """ APPs (OAuth) DB related """

    def build_app_dao(self):
        return AppDAO(self._logger, self._db_conn.get_conn_apps())
    
    def build_oauth_token_dao(self):
        return TokenDAO(self._logger, self._db_conn.get_conn_apps())

    def build_oauth_client_dao(self):
        return ClientDAO(self._logger, self._db_conn.get_conn_apps())
    
    def build_developers_dao(self):
        return DevelopersDAO(self._logger,  self._db_conn.get_conn_apps())

    """ Location DB related """
    
    def build_location_dao(self):
        return LocationDAO(self._logger, self._db_conn.get_conn_location())
