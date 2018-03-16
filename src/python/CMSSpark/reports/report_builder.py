class ReportBuilder():
    report = ''
    def append(self, value):
        ReportBuilder.report += value
    
    def get(self):
        return ReportBuilder.report