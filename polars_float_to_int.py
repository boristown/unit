import polars as pl
import numpy as np

class polars_float_to_int:
    def convertAuto(df:pl.DataFrame, col:str)->pl.DataFrame:
        min_v = df[col].min()
        max_v = df[col].max()
        if min_v is None or max_v is None:
            return df
        if min_v >= 0 and max_v <= np.iinfo(np.uint8).max:
            df = df.with_columns(df[col].cast(pl.UInt8))
        elif min_v >= 0 and max_v <= np.iinfo(np.uint16).max:
            df = df.with_columns(df[col].cast(pl.UInt16))
        elif min_v >= 0 and max_v <= np.iinfo(np.uint32).max:
            df = df.with_columns(df[col].cast(pl.UInt32))
        elif min_v >= 0 and max_v <= np.iinfo(np.uint64).max:
            df = df.with_columns(df[col].cast(pl.UInt64))
        elif min_v >= np.iinfo(np.int8).min and max_v <= np.iinfo(np.int8).max:
            df = df.with_columns(df[col].cast(pl.Int8))
        elif min_v >= np.iinfo(np.int16).min and max_v <= np.iinfo(np.int16).max:
            df = df.with_columns(df[col].cast(pl.Int16))
        elif min_v >= np.iinfo(np.int32).min and max_v <= np.iinfo(np.int32).max:
            df = df.with_columns(df[col].cast(pl.Int32))
        elif min_v >= np.iinfo(np.int64).min and max_v <= np.iinfo(np.int64).max:
            df = df.with_columns(df[col].cast(pl.Int64))
        else:
            df = df.with_columns(df[col].cast(pl.Float64))
        return df
    
if __name__ == "__main__":
    df = pl.DataFrame({
        "a": [1.0, 2.0, 3.3, -123123, 234234985734034],
        "b": [1, 2, 3, 4, None],
        "c": ["a", "b", "c", "d", "e"]
        })
    print(polars_float_to_int.convertAuto(df, "a"))
    print(polars_float_to_int.convertAuto(df, "b"))
