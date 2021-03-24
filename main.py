import pyodbc
import pandas as pd
from os import path


# Function for reading the user data base login information so they are not hardcoded in the code.
def read_login_db():
    if not path.exists("./login.json"):
        print("missing \"login.json\" file with login information in directory",
              "\nWrite data with format [{\"server\":\"SQLSERVER\\NAME\",\"db_name\":\"Survey_Sample_A19\"}]")
        exit()

    logs = pd.read_json("./login.json", orient='records')
    server = logs.loc[0]['server']
    db = logs.loc[0]['db_name']
    return server, db


# Connect to db, display error if can not
def connect_to_db(server, db):
    try:
        conn = pyodbc.connect('Driver={SQL Server};'
                              'Server=' + server + ';'
                                                   'Database=' + db + ';'
                                                                      'Trusted_Connection=yes;')
        print("connected to [" + server + "][" + db + "]")
    except pyodbc.Error as err:
        raise SystemExit("/!\ Connection failed. error:\n%s\nBe sure login.json is correctly filed" % err.args[1])
    return conn


def get_db_struct(conn):
    # Trying to reconstitute a table with all questions in all survey
    # the table in db used to join is incomplete, it doesn't include question not asked
    # (q4 is absent even though it is present in the expected result shown in class

    def cursor_struct_query(survey_id):
        # "1 AS IsAsked" when in the survey the question is asked, 0 when not
        query = ('SELECT DISTINCT s.SurveyId, q.QuestionId, sum( '
                 '    CASE  '
                 '      WHEN s.QuestionId = q.QuestionId THEN  1 '
                 '      ELSE 0  '
                 '    END) '
                 'AS IsAsked '
                 'FROM [Question] q, [SurveyStructure] s '
                 'WHERE s.SurveyId = %d '
                 'GROUP BY SurveyId, q.QuestionId') % survey_id
        return query

    # Table SurveyStructure only include Answers referenced, for instance Answer 4 is missing
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


# Generate the pivot query, works on two level, a subquery for the "SELECT level" with union of multiple table
# and "column level" checking if user has answered a question of a survey thanks to COALESCE
def set_pivot_query(db_struct):
    def set_query_column_level(id_survey, id_question, is_asked, id_question_max):
        if is_asked != 0:
            query_col = ("COALESCE (\n"
                         "    (   SELECT a.Answer_Value \n"
                         "        FROM [Answer] a \n"
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
                        "\nFROM [User] as u \nWHERE EXISTS (SELECT * FROM [Answer] AS a "
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
            # Check if for this survey the question is asked
            is_asked_id = db_struct[(db_struct["QuestionId"] == question_id) &
                                    (db_struct["SurveyId"] == survey_id)].loc[:, "IsAsked"].values
            mid_query += set_query_column_level(survey_id, question_id, is_asked_id, question_ids.max())
        query += set_query_select_level(survey_id, mid_query, survey_ids.min())
    print(query)
    return query


# Query to get the data from the answers
def get_all_data_from_answer():
    query = ('SELECT a.*, u.User_Name, u.User_Email, '
             'q.Question_Text,s.SurveyDescription, s.Survey_UserAdminId '
             'FROM  [Survey_Sample_A19].[dbo].[Answer] AS a '
             'INNER JOIN [Survey_Sample_A19].[dbo].[User] AS u '
             'ON a.UserId=u.UserId '
             'INNER JOIN [Survey_Sample_A19].[dbo].[Question] AS q '
             'ON a.QuestionId=q.QuestionId '
             'INNER JOIN [Survey_Sample_A19].[dbo].[Survey] AS s '
             'ON a.SurveyId=s.SurveyId')
    return query


# Is going to decide if we need to create or update or neither the view depending of csv file saved in current directory
def get_view(conn):
    def new_struct(df):
        df.to_csv("./struct_view.csv", sep=';')
        print("\nSaving new view of all data")
        dv = pd.read_sql_query(set_pivot_query(get_db_struct(conn)), conn)
        dv.to_csv("./saved_view.csv", sep=';')
        return dv

    # make new survey structure dataframe, as string because can differ with type from read_csv even with equal values
    new_struct_df = get_db_struct(conn).astype(str)
    # compare with old dataframe, update or create if needed, and return the up to data view
    if path.exists("./struct_view.csv"):
        old_struct_df = pd.read_csv("./struct_view.csv", sep=';',
                                    skiprows=[0], names=["SurveyId", "QuestionId", "IsAsked"]).astype(str)
        print("\nView of survey structure already saved:")
        if old_struct_df.equals(new_struct_df):
            if path.exists("./saved_view.csv"):
                print("\nView of survey structure hasn't been updated\nLoading previous view of all data")
                all_survey_df = pd.read_csv("./saved_view.csv", sep=';').iloc[:, 1:]
            else:
                print("\nMissing saved_view.csv file. Saving")
                all_survey_df = new_struct(new_struct_df)
        else:
            print("\nView of survey structure has been updated\nSaving the new structure")
            all_survey_df = new_struct(new_struct_df)
    else:
        print("\nNo view of survey structure is saved.\nSaving the current structure")
        all_survey_df = new_struct(new_struct_df)
    print("\n%s" % all_survey_df.head())


def main():
    server, db = read_login_db()
    read_login_db()
    conn = connect_to_db(server, db)
    get_view(conn)


if __name__ == "__main__":
    main()
