from datetime import datetime, timedelta
import re
import hashlib
import string
import time
import sys
from pyspark.sql import Window
from pyspark.sql.types import IntegerType, StringType, StructField, StructType, DateType, FloatType, DoubleType
from dateutil import relativedelta
from functools import partial
import pyspark.sql.functions as F


class EmptyDataFrameException(Exception):
    pass


class IllegalArgumentException(Exception):
    pass


class InvalidAggregationException(Exception):
    pass


class PivotException(Exception):
    pass


class TimeoutException(Exception):
    pass


class SparkUtils:
    
    def __init__(self):
        self.calendar_week = {'Monday': 0,
                              'Tuesday': 6,
                              'Wednesday': 5,
                              'Thursday': 4,
                              'Friday': 3,
                              'Saturday': 2,
                              'Sunday': 1}
    
    @staticmethod
    def get_dictionary_from_df(df, key_col_name, value_col_name):
        """
            This method creates a dictionary from 2 columns in a spark dataframe

            Returns
                --------
                Dictionary with keys and values provided
                ------
            Parameters
                --------
                df : spark dataframe
                    dataframe to pivot on
                key_col_name : string
                    dataframe column to act that acts as key in a dictionary
                value_col_name : string
                    dataframe column to act that acts as value in a dictionary
        """
        if key_col_name not in df.columns:
            raise IllegalArgumentException('Column {} does not exist in the dataframe'.format(key_col_name))
        if value_col_name not in df.columns:
            raise IllegalArgumentException('Column {} does not exist in the dataframe'.format(value_col_name))
        
        df = df.select(key_col_name, value_col_name).distinct().drop_duplicates(subset=[key_col_name])
        return dict(df.rdd.map(lambda x: (str(x[key_col_name]), str(x[value_col_name]))).collect())
    
    def get_week_start_date(self, date, start_day='Monday'):
        """
        Get a custom week start date of a datetime value

        Returns
            --------
            Date field with the starting day of the week
            ------
        Parameters
            --------
            date : datetime
                date to get the week start day
            start_day : basestring
                custom day name of the week, default is Monday
        """
        if start_day not in self.calendar_week.keys():
            raise IllegalArgumentException('Invalid start day, select correct week day')
        return date - timedelta((date.weekday() + self.calendar_week[start_day]) % 7)
    
    def add_week_start_date(self, df, date_col, new_date_col, start_day='Monday'):
        """
        Add a week start date (DateType) field as a new column to a dataframe. This method uses the get_week_start
        day method.

        Returns
            --------
            DataFrame with new week start date column
            ------
        Parameters
            --------
            df : spark dataframe
                name of the spark dataframe
            date_col : basestring
                column name of the date field
            new_date_col : basestring
                column name of the newly added column
            start_day : basestring
                day name of the custom start day
        """
        udf_change_date = F.udf(self.get_week_start_date, DateType())
        return df.withColumn(new_date_col, udf_change_date(df[date_col], F.lit(start_day)))
    
    @staticmethod
    def get_full_month_difference(date_1, date_2):
        return (date_1.year - date_2.year) * 12 + (date_1.month - date_2.month)
    
    def add_full_month_difference(self, df, date_1, date_2, month_diff_col_name):
        udf_full_month_difference = F.udf(self.get_full_month_difference, IntegerType())
        return df.withColumn(month_diff_col_name, udf_full_month_difference(df[date_1], df[date_2]))
    
    @staticmethod
    def get_files_read(df):
        file_column_name = 'FileRead_' + str(int(time.time()))
        df = df.withColumn(file_column_name, F.input_file_name())
        
        source_files_read = df.select(file_column_name).distinct().collect()
        source_files_read = [file.__getattr__(file_column_name) for file in source_files_read]
        
        return source_files_read
    
    @staticmethod
    def add_calendar_month_diff(df, date_col_1, date_col_2, new_col):
        return df.withColumn(new_col,
                             (F.year(F.col(date_col_1)) - F.year(F.col(date_col_2))) * 12 +
                             (F.month(F.col(date_col_1)) - F.month(F.col(date_col_2))))
    
    @staticmethod
    def change_timestamp_to_date(df, timestamp_col_name, date_col_name='', date_format='yyyy-mm-dd'):
        datatypes = dict(df.dtypes)
        
        if timestamp_col_name not in datatypes.keys():
            raise IllegalArgumentException(
                'Timestamp column {} does not exist, please check'.format(timestamp_col_name))
        elif 'timestamp' not in datatypes[timestamp_col_name].lower():
            raise IllegalArgumentException(
                'Datatype of the column {} is not Timestamp type, please check'.format(timestamp_col_name))
        
        if date_col_name is None or date_col_name == '':
            date_col_name = timestamp_col_name
        df = df.withColumn(date_col_name, F.date_format(F.col(timestamp_col_name), date_format).cast('date'))
        return df
    
    @staticmethod
    def add_hashed_id(df, columns=[], hashed_col='Hashed_ID', hash_type='md5'):
        """
            This method will create a dummy transaction for each account record in the dataframe.

            Returns
                --------
                Dataframe with hashed Id as a column
                ------
            Parameters
                --------
                df : spark dataframe
                    dataframe to create hashed id on
                columns : list of strings
                    columns to use hash, default is None which takes in all columns of df
                hashed_col : string
                    column name for hashed id
                --------
        """
        if len(columns) == 0:
            columns = df.columns
        else:
            illegal_columns = []
            for column in columns:
                if column not in df.columns:
                    illegal_columns.append(column)
            if len(illegal_columns) > 0:
                raise IllegalArgumentException(
                    'Column {} does not exist in dataframe'.format(', '.join(illegal_columns)))
        
        if hashed_col is None or hashed_col == '':
            hashed_col = 'Hashed_ID'
        
        if hash_type == 'md5':
            df = df.withColumn(hashed_col, F.md5(F.concat(*columns)))
        else:
            df = df.withColumn(hashed_col, F.sha2(F.concat(*columns)))
        return df
    
    @staticmethod
    def pivot_dataframe(df, group_by_cols, pivot_col, value_col, selected_pivot_values=None,
                        aggr='max', rename_prefix=None, rename_suffix=None):
        # TODO : Update aggregation and other details for multiple column pivots and filtering pivot columns.
        #  Also add a functionality to rename pivoted columns
        
        """
            This method pivots a dataframe based on the columns provided.

            Returns
                --------
                Pivoted dataframe
                ------
            Parameters
                --------
                df : spark dataframe
                    dataframe to pivot on
                group_by_cols : string or list of strings
                    Index columns
                pivot_col : string
                    pivot column
                value_col : string
                    Aggregation column
                selected_pivot_values : list of strings
                    Values of pivot column to consider
                aggr : string ('max', 'min', 'sum', 'avg')
                    Aggregation to perform on value_col
                rename_prefix : string
                    To add prefix to the resulted pivoted columns. Include space after the value if needed
                rename_suffix : string
                    To add suffix to the resulted pivoted columns. Include space before the value if needed
                --------
        """
        file_schema_dict = dict(df.dtypes)
        value_col_schema = file_schema_dict[value_col].lower()
        
        if aggr not in ['max', 'min', 'sum', 'avg', 'count', 'countDistinct']:
            raise IllegalArgumentException(
                'Invalid aggregation string {}. It should be one of (max, min, sum, avg, count, '
                'countDistinct)'.format(aggr))
        
        elif aggr in ['sum', 'avg'] and value_col_schema not in ['int',
                                                                 'bigint',
                                                                 'long',
                                                                 'float',
                                                                 'double']:
            raise InvalidAggregationException(
                'Pivot aggregation is {} but column {} is not numeric, it is {}'.format(aggr,
                                                                                        value_col,
                                                                                        value_col_schema))
        if df.count() == 0:
            raise EmptyDataFrameException('Dataframe to be pivoted is empty, please check')
        
        if df.select(pivot_col).distinct().count() >= 1023:
            raise PivotException(
                'Pivot column {} has more than 1024 distinct values, please filter it'.format(pivot_col))
        
        if selected_pivot_values is not None:
            df_pivoted = df.filter(df[pivot_col].isin(selected_pivot_values)) \
                .groupBy(group_by_cols) \
                .pivot(pivot_col) \
                .agg({value_col: aggr})
        else:
            df_pivoted = df.groupBy(group_by_cols) \
                .pivot(pivot_col) \
                .agg({value_col: aggr})
        
        if rename_prefix is not None and rename_prefix != '':
            for c in df_pivoted.columns:
                if c not in group_by_cols:
                    df_pivoted = df_pivoted.withColumnRenamed(c, rename_prefix + c)
        
        if rename_suffix is not None and rename_suffix != '':
            for c in df_pivoted.columns:
                if c not in group_by_cols:
                    df_pivoted = df_pivoted.withColumnRenamed(c, c + rename_suffix)
        
        return df_pivoted
    
    @staticmethod
    def unpivot_dataframe(df, index_cols, unpivot_cols):
        # *****INCOMPLETE FUNCTION - DO NOT USE*****
        # TODO : Unpivot based on multiple columns

        """
            This method melts(unpivots) a dataframe based on the columns provided.

            Returns
                --------
                Melted dataframe
                ------
            Parameters
                --------
                df : spark dataframe
                    dataframe to melt
                index_cols : string or list of strings
                    Index columns
                unpivot_cols : string or list of strings
                    columns to consider for melting
                --------
        """
        cols, dtypes = zip(*((c, t) for (c, t) in df.types if c in unpivot_cols))
        
        assert len(set(dtypes)) == 1, 'All columns to melt should be of same data type'
        
        _vars_and_vals = F.explode(F.array([F.struct(F.lit(c).alias("key"),
                                                     F.col(c).alias("val")) for c in cols])).alias("_vars_and_vals")
        
        return df.select(index_cols + [_vars_and_vals]).select(index_cols + ["_vars_and_vals.key",
                                                                             "_vars_and_vals.val"])
    
    def rename_cols(agg_df, ignore_first_n=1):
        """changes the default spark aggregate names `avg(colname)`
        to something a bit more useful. Pass an aggregated dataframe
        and the number of aggregation columns to ignore.
        """
        delimiters = "(", ")"
        split_pattern = '|'.join(map(re.escape, delimiters))
        splitter = partial(re.split, split_pattern)
        split_agg = lambda x: '_'.join(splitter(x))[0:-ignore_first_n]
        renamed = map(split_agg, agg_df.columns[ignore_first_n:])
        renamed = zip(agg_df.columns[ignore_first_n:], renamed)
        for old, new in renamed:
            agg_df = agg_df.withColumnRenamed(old, new)
        return agg_df
    
    @staticmethod
    def get_ordered_dataframe(df, partition_cols, order_col, asc=True, drop_dups=True):
        partition_cols = [F.col(x) for x in partition_cols]
        
        if asc:
            window = Window.partitionBy(partition_cols).orderBy(F.col(order_col).asc())
        else:
            window = Window.partitionBy(partition_cols).orderBy(F.col(order_col).desc())
        
        df_partitioned = df.row_number().over(window).alias('tie_breaker')
        
        if drop_dups:
            return df_partitioned.filter(F.col('tie_breaker') == 1).drop('tie_breaker')
        else:
            return df_partitioned.drop('tie_breaker')
    
    @staticmethod
    def get_concatenated_string_byrows(df, group_by_col, value_col, new_col_name, delimiter=','):
        datatypes = dict(df.dtypes)
        
        if group_by_col not in datatypes.keys():
            raise IllegalArgumentException('Groupby column does not exist, please check')
        elif value_col not in datatypes.keys():
            raise IllegalArgumentException('Value column does not exist, please check')
        elif 'string' not in datatypes[value_col].lower():
            raise IllegalArgumentException('Datatype of the column provided is not string, please check')
        
        df_concatenated = df.groupBy(group_by_col) \
            .agg(F.concat_ws(delimiter + ' ', F.collect_list(value_col)) \
                 .alias(new_col_name))
        
        return df_concatenated
    
    @staticmethod
    def get_concatenated_distinct_string_byrows(df, group_by_col, value_col, new_col_name, delimiter=','):
        datatypes = dict(df.dtypes)
        
        if group_by_col not in datatypes.keys():
            raise IllegalArgumentException('Groupby column does not exist, please check')
        elif value_col not in datatypes.keys():
            raise IllegalArgumentException('Value column does not exist, please check')
        elif 'string' not in datatypes[value_col].lower():
            raise IllegalArgumentException('Data type of the column provided is not string, please check')
        
        df_concatenated = df.groupBy(group_by_col) \
            .agg(F.concat_ws(delimiter + ' ', F.collect_set(value_col)) \
                 .alias(new_col_name))
        
        return df_concatenated
    
    
    def concatenate_dataframes(dfs):
    return functools.reduce(DataFrame.union, dfs)


    def forward_fill_dataframe(df, partition_cols, filling_cols):
        forward_fill_window = Window.partitionBy(partition_cols).rowsBetween(-sys.maxsize, 0)
        for column in filling_cols:
            filled_column_values = F.last(df[column], ignorenulls=True).over(forward_fill_window)
            df = df.withColumn(column, filled_column_values)

        return df


    def back_fill_dataframe(df, partition_col, columns_to_fill):
        df = df.withColumn('Dummy_ID', F.monotonically_increasing_id())
        # backfilled_df = df.withColumn('Dummy_ID', F.monotonically_increasing_id())
        backfilled_final_df = df.select('Dummy_ID', 'PropHash')
        for column_to_fill in columns_to_fill:
            df_a = df.select('Dummy_ID', 'PropHash', column_to_fill)
            df_b = df.select('Dummy_ID', 'PropHash', column_to_fill)
            backfilled_df = df_a.crossJoin(df_b).where((df_a[partition_col] >= df_b[partition_col]) &
                                                       (df_a[column_to_fill].isNotNull() |
                                                        df_b[column_to_fill].isNotNull()))
            #for c in backfilled_df.columns:
            print('Column is {}'.format(column_to_fill))
            back_fill_window = Window.partitionBy(df_a[partition_col]).orderBy(df_b[partition_col])
            backfilled_df = backfilled_df.withColumn('row_num', F.row_number().over(back_fill_window))
            backfilled_df = backfilled_df.filter(F.col('row_num') == 1)

            backfilled_df = backfilled_df.select(df_a.Dummy_ID, df_a.PropHash,
                                                 F.coalesce(df_a[column_to_fill], df_b[column_to_fill]).alias(column_to_fill))
            backfilled_final_df = backfilled_final_df.join(backfilled_df,
                                                           ['Dummy_ID', 'PropHash'],
                                                           'left')
        return backfilled_final_df

