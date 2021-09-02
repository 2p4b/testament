defmodule Testament.Repo.Migrations.CreateEvents do
    use Ecto.Migration

    def change do
        create table(:events) do
            add :uuid,              :uuid,      unique: true
            add :type,              :string
            add :topic,             :string
            add :position,          :integer
            add :stream_id,         :uuid
            add :data,              :binary
            add :causation_id,      :string
            add :correlation_id,    :string
            add :timestamp,         :utc_datetime_usec
            timestamps(created_at: false, updated_at: false)
        end

        rename table("events"), :id, to: :number
    end
end
