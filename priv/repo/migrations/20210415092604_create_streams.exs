defmodule Testament.Repo.Migrations.CreateStreams do
    use Ecto.Migration

    def change do
        create table(:streams, primary_key: false) do
            add :id,        :string,    primary_key: true
            add :version,   :integer
        end
    end
end
