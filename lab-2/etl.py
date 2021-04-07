#branching pipelines
import apache_beam as beam

with beam.Pipeline() as p:
  input_collection = (
      p
      | 'reading department data' >> beam.io.ReadFromText('dept_data.txt')
      | 'split rows' >> beam.Map(lambda x: x.split(','))
  )

  sales_attendance = (
      input_collection
      | 'filter for sales' >> beam.Filter(lambda x: x[3]=='Sales')
      | 'pair each sales employee with 1' >> beam.Map(lambda x: ("Sales, " + x[1],1))
      | 'Grouping and Sum1' >> beam.CombinePerKey(sum)
  )

  hr_attendance = (
      input_collection
      | 'filter for HR' >> beam.Filter(lambda x: x[3]=='HR')
      | 'pair each HR employee with 1' >> beam.Map(lambda x: ("HR, "+x[1],1))
      | 'Grouping and Sum' >> beam.CombinePerKey(sum)
  )

  output = (
      (sales_attendance, hr_attendance)
      | 'Merging two PCollections' >> beam.Flatten()
      | 'Writing output' >> beam.io.WriteToText("data/attendance_results")
  )


