defmodule Testament.Repo.Migrations.CreateEvents do
    use Ecto.Migration

    def change do
        create table(:events, primary_key: false) do
            add :uuid,              :uuid,      primary_key: true
            add :number,            :integer,   unique: true
            add :type,              :string
            add :topic,             :string
            add :reduction,         :integer
            add :stream_type,       :string
            add :stream_id,         :string
            add :payload,           :binary
            add :causation_id,      :string
            add :correlation_id,    :string
            add :timestamp,         :utc_datetime_usec
            timestamps(created_at: false, updated_at: false)
        end

    end
end
