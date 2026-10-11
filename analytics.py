
import pandas as pd

data = {
    "student_id": [101, 102, 103, 104],
    "marks": [85, 72, 91, 65]
}

df = pd.DataFrame(data)

print("STUDENT MARKS ANALYTICS")
print("-----------------------")
print(df.to_string(index=False))
print("\nAverage marks:", df["marks"].mean())
print("Highest marks:", df["marks"].max())
print("Lowest marks:", df["marks"].min())
print("Students scoring 75 or above:", (df["marks"] >= 75).sum())
