// Task 1: Top 5 frequently occurring comments
db.airflow_data.aggregate([
  {
    $match: {
      content: { $type: "string", $ne: "" }
    }
  },
  {
    $group: {
      _id: "$content",
      cnt: { $sum: 1 }
    }
  },
  { $sort: { cnt: -1 } },
  { $limit: 5 },
  {
    $project: {
      _id: 0,
      content: "$_id",
      cnt: 1
    }
  }
]);


// Task 2: All entries where the "content" field is less than 5 characters long
db.airflow_data.aggregate([
  {
    $match: {
      content: { $type: "string" }
    }
  },
  {
    $match: {
      $expr: {
        $lt: [
          { $strLenCP: { $trim: { input: "$content" } } },
          5
        ]
      }
    }
  },
  {
    $project: {
      content: 1,
      at: 1,
      score: 1,
      userName: 1,
      len: { $strLenCP: { $trim: { input: "$content" } } }
    }
  }
]);


// Task 3: Average rating per day (result returned as timestamp / Date)
db.airflow_data.aggregate([
  {
    $addFields: {
      at_dt: {
        $dateFromString: {
          dateString: "$at",
          format: "%Y-%m-%d %H:%M:%S",
          timezone: "UTC"
        }
      }
    }
  },
  {
    $addFields: {
      day: {
        $dateTrunc: {
          date: "$at_dt",
          unit: "day",
          timezone: "UTC"
        }
      }
    }
  },
  {
    $group: {
      _id: "$day",
      avgScore: { $avg: "$score" },
      cnt: { $sum: 1 }
    }
  },
  { $sort: { _id: 1 } },
  {
    $project: {
      _id: 0,
      day: "$_id",
      avgScore: { $round: ["$avgScore", 2] },
      cnt: 1
    }
  }
]);
