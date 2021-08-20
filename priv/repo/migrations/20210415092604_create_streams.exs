defmodule Testament.Repo.Migrations.CreateStreams do
    use Ecto.Migration

    def change do
        create table(:streams, primary_key: false) do
            add :id,        :string,      primary_key: true
            add :type,      :string
            add :position,  :integer
            timestamps()
        end
    end
end
