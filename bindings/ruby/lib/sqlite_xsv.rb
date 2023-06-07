require "version"

module SqliteXsv
  class Error < StandardError; end
  def self.xsv_loadable_path
    File.expand_path('../xsv0', __FILE__)
  end
  def self.load(db)
    db.load_extension(self.xsv_loadable_path)
  end
end
