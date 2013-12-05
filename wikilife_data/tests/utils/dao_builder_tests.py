# coding=utf-8

from wikilife_data.tests.base_test import BaseTest

class DAOBuilderTests(BaseTest):

    _dao_bldr = None

    def setUp(self):
        self._dao_bldr = self.get_dao_builder()
    
    def tearDown(self):
        self._dao_bldr = None
            
    def assert_class_full_name(self, instance, class_full_name):
        assert instance != None
        assert "%s.%s" % (instance.__class__.__module__, instance.__class__.__name__) == class_full_name


    """ Main DB related"""
    
    """ live """    
    def test_build_live_meta_dao(self):
        self.assert_class_full_name(self._dao_bldr.build_live_meta_dao(), "wikilife_data.managers.meta.meta_manager.MetaManager")

    def test_build_live_question_dao(self):
        self.assert_class_full_name(self._dao_bldr.build_live_question_dao(), "wikilife_data.managers.questions.question_manager.QuestionManager") 

    def test_build_live_questions_tag_dao(self):
        self.assert_class_full_name(self._dao_bldr.build_live_questions_tag_dao(), "wikilife_data.managers.questions.questions_tag_manager.QuestionsTagManager") 
    
    def test_build_live_security_question_dao(self):
        self.assert_class_full_name(self._dao_bldr.build_live_security_question_dao(), "wikilife_data.managers.security_questions.security_question_manager.SecurityQuestionManager")
    
    """ edit """
    def test_build_edit_meta_dao(self):
        self.assert_class_full_name(self._dao_bldr.build_edit_meta_dao(), "wikilife_data.managers.meta.meta_manager.MetaManager")
    
    def test_build_write_meta_dao(self):
        self.assert_class_full_name(self._dao_bldr.build_write_meta_dao(), "wikilife_data.managers.meta.write_meta_manager.WriteMetaManager")
    
    def test_build_edit_question_dao(self):
        self.assert_class_full_name(self._dao_bldr.build_edit_question_dao(), "wikilife_data.managers.questions.question_manager.QuestionManager") 

    def test_build_edit_questions_tag_dao(self):
        self.assert_class_full_name(self._dao_bldr.build_edit_questions_tag_dao(), "wikilife_data.managers.questions.questions_tag_manager.QuestionsTagManager") 
    
    def test_build_edit_security_question_dao(self):
        self.assert_class_full_name(self._dao_bldr.build_edit_security_question_dao(), "wikilife_data.managers.security_questions.security_question_manager.SecurityQuestionManager")


    """ Users DB related """
    
    def test_build_user_dao(self):
        self.assert_class_full_name(self._dao_bldr.build_user_dao(), "wikilife_data.managers.users.user_manager.UserManager")

    def test_build_twitter_user_dao(self):
        self.assert_class_full_name(self._dao_bldr.build_twitter_user_dao(), "wikilife_data.managers.crawler.twitter.twitter_user_manager.TwitterUserManager")
    
    def test_build_user_token_dao(self):
        self.assert_class_full_name(self._dao_bldr.build_user_token_dao(), "wikilife_data.managers.user_token.user_token_manager.UserTokenManager")

    def test_build_recovery_information_dao(self):
        self.assert_class_full_name(self._dao_bldr.build_recovery_information_dao(), "wikilife_data.managers.recovery_information.recovery_information_manager.RecoveryInformationManager")
    
    def test_build_twitter_configuration_dao(self):
        self.assert_class_full_name(self._dao_bldr.build_twitter_configuration_dao(), "wikilife_data.managers.configuration.twitter_configuration_manager.TwitterConfigurationManager")
    

    """ Logs DB related """

    def test_build_log_dao(self):
        self.assert_class_full_name(self._dao_bldr.build_log_dao(), "wikilife_data.dao.logs.log_dao.LogDAO")

    def test_build_answer_dao(self):
        self.assert_class_full_name(self._dao_bldr.build_answer_dao(), "wikilife_data.managers.answers.answer_manager.AnswerManager")


    """ Admin DB related """

    def test_build_admin_user_dao(self):
        self.assert_class_full_name(self._dao_bldr.build_admin_user_dao(), "wikilife_data.managers.admin.admin_user_manager.AdminUserManager")

    def test_build_staging_dao(self):
        self.assert_class_full_name(self._dao_bldr.build_staging_dao(), "wikilife_data.managers.admin.staging_manager.StagingManager")

    def test_build_release_dao(self):
        self.assert_class_full_name(self._dao_bldr.build_release_dao(), "wikilife_data.managers.admin.release_manager.ReleaseManager")

    def test_build_history_dao(self):
        self.assert_class_full_name(self._dao_bldr.build_history_dao(), "wikilife_data.managers.admin.history_manager.HistoryManager")


    """ Crawler DB related """
    
    def test_build_twitter_reader_dao(self):
        self.assert_class_full_name(self._dao_bldr.build_twitter_reader_dao(), "wikilife_data.managers.crawler.twitter.twitter_reader_manager.TwitterReaderManager")


    """ Processors DB related """

    def test_build_processor_status_dao(self):
        self.assert_class_full_name(self._dao_bldr.build_processor_status_dao(), "wikilife_data.managers.processors.processor_status_manager.ProcessorStatusManager")
    
    def test_build_final_log_processor_status_dao(self):
        self.assert_class_full_name(self._dao_bldr.build_final_log_processor_status_dao(), "wikilife_data.managers.processors.final_log_processor_status_manager.FinalLogProcessorStatusManager")
    
    def test_build_profile_dao(self):
        self.assert_class_full_name(self._dao_bldr.build_profile_dao(), "wikilife_data.managers.profile.profile_manager.ProfileManager") 
    
    def test_build_timeline_dao(self):
        self.assert_class_full_name(self._dao_bldr.build_timeline_dao(), "wikilife_data.managers.timeline.timeline_manager.TimelineManager")

    def test_build_stats_dao(self):
        self.assert_class_full_name(self._dao_bldr.build_stats_dao(), "wikilife_data.managers.stats.stats_manager.StatsManager") 

    def test_build_daily_stats_dao(self):
        self.assert_class_full_name(self._dao_bldr.build_daily_stats_dao(), "wikilife_data.managers.stats.daily_stats_manager.DailyStatsManager")
    
    def test_build_generic_stats_dao(self):
        self.assert_class_full_name(self._dao_bldr.build_generic_stats_dao(), "wikilife_data.managers.stats.generic_stats_manager.GenericStatsManager")
    
    def test_build_reports_dao(self):
        self.assert_class_full_name(self._dao_bldr.build_reports_dao(), "wikilife_data.managers.reports.reports_manager.ReportsManager")  

    def test_build_generic_daily_stats_dao(self):
        self.assert_class_full_name(self._dao_bldr.build_generic_daily_stats_dao(), "wikilife_data.managers.stats.generic_daily_stats_manager.GenericDailyStatsManager")

    def test_build_final_log_dao(self):
        self.assert_class_full_name(self._dao_bldr.build_final_log_dao(), "wikilife_data.managers.final_logs.final_log_manager.FinalLogManager")

    def test_build_user_log_stats_dao(self):
        self.assert_class_full_name(self._dao_bldr.build_user_log_stats_dao(), "wikilife_data.managers.stats.user_log_stats_manager.UserLogStatsManager")

    def test_build_generic_global_log_stats_dao(self):
        self.assert_class_full_name(self._dao_bldr.build_generic_global_log_stats_dao(), "wikilife_data.managers.stats.generic_global_log_stats_manager.GenericGlobalLogStatsManager")

    def test_build_profile_report_dao(self):
        self.assert_class_full_name(self._dao_bldr.build_profile_report_dao(), "wikilife_data.managers.profile.profile_report_manager.ProfileReportManager")
