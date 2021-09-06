defmodule Testament.Repo.Migrations.CreateEvents do
    use Ecto.Migration

    def change do
        create table(:events, primary_key: false) do
            add :number,            :integer,   primary_key: true, unique: true
            add :uuid,              :uuid,      unique: true
            add :type,              :string
            add :topic,             :string
            add :position,          :integer
            add :stream,            :string
            add :data,              :binary
            add :causation_id,      :string
            add :correlation_id,    :string
            add :timestamp,         :utc_datetime_usec
        end
    end
end
