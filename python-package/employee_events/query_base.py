# Import any dependencies needed to execute sql queries
from sqlite3 import connect
import pandas as pd
from .sql_execution import QueryMixin

# Define a class called QueryBase
# Use inheritance to add methods
# for querying the employee_events database.
class QueryBase(QueryMixin):

    # Create a class attribute called `name`
    # set the attribute to an empty string
    name = ""

    # Define a `names` method that receives
    # no passed arguments
    @staticmethod
    def names():
        
        # Return an empty list
        return []


    # Define an `event_counts` method
    # that receives an `id` argument
    # This method should return a pandas dataframe
    def event_counts(self, id):
        """
        Returns a pandas DataFrame with the count of positive and negative events
        grouped by event_date, ordered by event_date.
        """
        # QUERY 1
        # Write an SQL query that groups by `event_date`
        # and sums the number of positive and negative events
        # Use f-string formatting to set the FROM {table}
        # to the `name` class attribute
        # Use f-string formatting to set the name
        # of id columns used for joining
        # order by the event_date column
        # Compose SQL query using f-string and class attribute `name`
        sql_query = f"""
            SELECT 
                ee.event_date,
                SUM(ee.positive_events) AS positive_events,
                SUM(ee.negative_events) AS negative_events
            FROM employee_events ee
            JOIN {self.name} e ON ee.employee_id = e.employee_id
            WHERE ee.employee_id = {id}
            GROUP BY ee.event_date
            ORDER BY ee.event_date
        """
        return self.pandas_query(sql_query)


    # Define a `notes` method that receives an id argument
    # This function should return a pandas dataframe
    def notes(self, id):
        """
        Returns a pandas DataFrame with note_date and note for the given id,
        joining the notes table and the table named in `name`.
        """

        # QUERY 2
        # Write an SQL query that returns `note_date`, and `note`
        # from the `notes` table
        # Set the joined table names and id columns
        # with f-string formatting
        # so the query returns the notes
        # for the table name in the `name` class attribute
        sql_query = f"""
            SELECT notes.note_date, notes.note
            FROM notes
            JOIN {self.name} ON notes.{id} = {self.name}.{id}
            WHERE {self.name}.{id} = ?
            ORDER BY notes.note_date
        """

        with connect(db_path) as conn:
            return pd.read_sql_query(sql_query, conn, params=(id,))

