defmodule Testament.Repo.Migrations.CreateEvents do
    use Ecto.Migration

    def change do
        create table(:events, primary_key: false) do
            add :number,            :integer,   primary_key: true, unique: true
            add :uuid,              :uuid,      unique: true
            add :topic,             :string
            add :position,          :integer
            add :data,              :text
            add :stream_id,         :string
            add :causation_id,      :string
            add :correlation_id,    :string
            add :timestamp,         :utc_datetime_usec
        end
    end
end
