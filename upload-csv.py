def read_file(self, filename):
    self.response.write('Reading the full file contents:\n')
    
    gcs_file = gcs.open(filename)
    contents = gcs_file.read()
    gcs_file.close()
    self.response.write(contents)
