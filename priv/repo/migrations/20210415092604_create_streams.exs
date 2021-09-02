defmodule Testament.Repo.Migrations.CreateStreams do
    use Ecto.Migration

    def change do
        create table(:streams, primary_key: false) do
            add :uuid,      :uuid,      primary_key: true
            add :id,        :string
            add :type,      :string
            add :position,  :integer
            timestamps()
        end
    end
end
