import pyodbc
import pandas as pd
from os import path


def connect_to_db(server, db):
    try:
        conn = pyodbc.connect('Driver={SQL Server};'
                              'Server=' + server + ';'
                                                   'Database=' + db + ';'
                                                                      'Trusted_Connection=yes;')
        print("connected to [" + server + "][" + db + "]")
    except pyodbc.Error as err:
        raise SystemExit("/!\ Connection failed. error:\n%s" % err.args[1])
    return conn


def get_db_struct(conn):
    def cursor_struct_query(survey_id):
        # "1 AS IsAsked" when in the survey the question is asked, 0 when not
        query = ('SELECT DISTINCT s.SurveyId, q.QuestionId, sum( '
                 '    CASE  '
                 '      WHEN s.QuestionId = q.QuestionId THEN  1 '
                 '      ELSE 0  '
                 '    END) '
                 'AS IsAsked '
                 'FROM Question q, SurveyStructure s '
                 'WHERE s.SurveyId = %d '
                 'GROUP BY SurveyId, q.QuestionId') % survey_id
        return query

    # Table SurveyStructure only include Questions referenced, for instance question 4 is missing
    # A first cursor to get the structure by survey
    c1 = conn.cursor()
    c1.execute("SELECT SurveyId FROM Survey ORDER BY SurveyId")
    # Will be saved in three columns, survey and question ids and a third to check if question is asked
    df = pd.DataFrame(columns=["SurveyId", "QuestionId", "IsAsked"])
    # First loop to access each survey then second loop for each row, each row added to the dataframe
    for s_id in c1.fetchall():
        c2 = conn.cursor()
        c2.execute(cursor_struct_query(s_id[0]))
        for SurveyId, QuestionId, IsAsked in c2.fetchall():
            df = df.append(pd.DataFrame([[SurveyId, QuestionId, IsAsked]],
                                        columns=["SurveyId", "QuestionId", "IsAsked"]), ignore_index=True)
        c2.close()
    c1.close()
    return df


def set_pivot_query(db_struct):
    def set_query_column_level(id_survey, id_question, is_asked, id_question_max):
        if is_asked != 0:
            query_col = ("COALESCE (\n"
                         "    (   SELECT a.Answer_Value \n"
                         "        FROM Answer a \n"
                         "        WHERE a.UserId = u.UserId \n"
                         "        AND a.SurveyId = %d AND a.QuestionId = %d \n"
                         "    ), NULL) AS Answer_%d\n") % (id_survey, id_question, id_question)
        else:
            query_col = "NULL AS Answer_%d" % id_question
        if id_question < id_question_max:
            query_col = query_col + ", "
        return query_col

    def set_query_select_level(id_survey, column_query, id_min):
        query_select = ("SELECT UserId, %s as SurveyId, %s "
                        "\nFROM [User] as u WHERE EXISTS (SELECT * FROM Answer as a "
                        "WHERE u.UserId = a.UserId AND a.SurveyId = %s)") % (id_survey, column_query, id_survey)
        if id_survey != id_min:
            query_select = " \nUNION " + query_select
        return str(query_select)

    question_ids = db_struct["QuestionId"].unique()
    survey_ids = db_struct["SurveyId"].unique()
    # Initiate query_select string, is going to be filled with column depending if each question asked for each survey
    query = ""
    for survey_id in survey_ids:
        mid_query = ""
        for question_id in question_ids:
            # We check if for this survey, the question is asked and get the appropriate part of the query
            is_asked_id = db_struct[(db_struct["QuestionId"] == question_id) &
                                    (db_struct["SurveyId"] == survey_id)].loc[:, "IsAsked"].values
            mid_query += set_query_column_level(survey_id, question_id, is_asked_id, question_ids.max())
        query += set_query_select_level(survey_id, mid_query, survey_ids.min())
    return query


def get_all_data_from_answer():
    query = ('SELECT a.*, u.User_Name, u.User_Email, '
             'q.Question_Text,s.SurveyDescription, s.Survey_UserAdminId '
             'FROM  [Survey_Sample_A19].[dbo].[Answer] As a '
             'INNER JOIN [Survey_Sample_A19].[dbo].[User] As u '
             'ON a.UserId=u.UserId '
             'INNER JOIN [Survey_Sample_A19].[dbo].[Question] As q '
             'ON a.QuestionId=q.QuestionId '
             'INNER JOIN [Survey_Sample_A19].[dbo].Survey As s '
             'ON a.SurveyId=s.SurveyId')
    return query


def df_to_csv(df, name):
    df.to_csv('.\ ' + name + '_view.csv')
    print("\n[%s_view.csv] saved"%name)


def check_view(conn):
    def new_struct(df):
        df.to_csv("./struct_view.csv", sep=';')
        print("\nSaving new view of all data")
        dv = pd.read_sql_query(set_pivot_query(get_db_struct(conn)), conn)
        dv.to_csv("./saved_view.csv", sep=';')
        return dv

    # make new survey structure dataframe, as string because can differ with type from read_csv even with equal values
    new_struct_df = get_db_struct(conn).astype(str)
    # compare with old dataframe, update if needed, and return the up to data view
    if path.exists("./struct_view.csv"):
        old_struct_df = pd.read_csv("./struct_view.csv", sep=';',
                                    skiprows=[0], names=["SurveyId", "QuestionId", "IsAsked"]).astype(str)
        print("\nView of survey structure already saved:")
        if old_struct_df.equals(new_struct_df):
            print("\nView of survey structure hasn't been updated\nLoading previous view of all data")
            all_survey_df = pd.read_csv("./saved_view.csv", sep=';').iloc[:, 1:]
        else:
            print("\nView of survey structure has been updated\nSaving the new structure")
            all_survey_df = new_struct(new_struct_df)
    else:
        print("\nNo view of survey structure is saved.\nSaving the current structure")
        all_survey_df = new_struct(new_struct_df)
    print("\n%s" % all_survey_df.head())


def main():
    server = 'DESKTOP-50BQN97\SQL2019'
    db = 'Survey_Sample_A19'
    conn = connect_to_db(server, db)
    check_view(conn)

if __name__ == "__main__":
    main()
