import pandas as pd
import os

INPUT_EVENTS = "job_events.csv"  #输入事件
OUTPUT_FEATURES = "job_features_1d.csv"  #输出特征

if not os.path.exists(INPUT_EVENTS):        
print("Please update the INPUT_EVENTS path.")
else:    
    df = pd.read_csv(INPUT_EVENTS)
    df["event_time"] = pd.to_datetime(df["event_time"]) #统一时间类型
    df["dt"] = df["event_time"].dt.date        
  
#呈现 ClickHouse 的聚合逻辑         
def agg(group):                
return pd.Series({                        
"impression_1d": (group["event_type"] == "impression").sum(),                        
"view_1d":       (group["event_type"] == "view").sum(),                        
"apply_1d":      (group["event_type"] == "apply").sum(),                        
"hire_1d":       (group["event_type"] == "hire").sum(),                        
"uniq_user_1d":  group["user_id"].nunique(),                
})        

    features = (
        df                     
.groupby(["dt", "job_id", "company_id"])                     
.apply(agg)                     
.reset_index()             
)
    features.to_csv(OUTPUT_FEATURES, index=False)    

print("rows:", len(features))        
print(features.head())
